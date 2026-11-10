"""Write the reduced landing extract and the SQL the browser runs.

The parquet files come from ``mercadoalba.generate``. The statements come from
``mercadoalba.sql_builder`` and ``mercadoalba.quality``. Expectations are
measured by ``run_pipeline`` on that extract, then checked again by executing
the exported SQL in a fresh DuckDB file.
"""

from __future__ import annotations

import json
import shutil
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from mercadoalba.calendar_mx import dim_date_rows
from mercadoalba.config import Settings
from mercadoalba.generate import generate_days
from mercadoalba.pipeline import run_pipeline
from mercadoalba.quality import build_checks
from mercadoalba.schema import (
    DIM_CUSTOMER,
    DIM_DATE,
    DIM_PRODUCT,
    DIM_STORE,
    RAW_BY_ENTITY,
    column_names,
)
from mercadoalba.sql_builder import (
    SqlContext,
    dates_predicate,
    delete_dim_date_sql,
    duckdb_index_sqls,
    enriched_table_sql,
    merge_dimension_sql,
    merge_fact_sql,
    replace_fact_sql,
    schema_statements,
    stage_customers_sql,
    stage_inventory_sql,
    stage_order_lines_sql,
    stage_products_sql,
    stage_stores_sql,
)
from mercadoalba.warehouse import DuckDBWarehouse

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "site" / "public" / "sample"
LANDING = SAMPLE / "landing"
CORRECTIONS = SAMPLE / "corrections"
CALENDAR = SAMPLE / "calendar"

START = date(2026, 3, 2)
DAYS = tuple(START + timedelta(days=offset) for offset in range(5))
CORRECTION_DAY = date(2026, 3, 4)
LOGICAL_END = DAYS[-1]

CTX = SqlContext("duckdb")
ENTITIES = ("order_lines", "products", "customers", "stores", "daily_inventory")


def _settings(duckdb_path: Path) -> Settings:
    return Settings(
        duckdb_path=duckdb_path,
        landing_root=LANDING,
        seed=42,
        products=12,
        stores=3,
        customers=24,
        order_lines_per_day=40,
        lookback_days=0,
        transform_attempts=1,
        vary_volume_by_weekday=True,
    )


def _window(logical: date, lookback: int) -> tuple[date, ...]:
    start = logical - timedelta(days=lookback)
    return tuple(day for day in DAYS if start <= day <= logical)


def _parquet_path(entity: str, day: date, *, correction: bool) -> str:
    if correction and entity == "order_lines" and day == CORRECTION_DAY:
        return f"corrections/order_lines/dt={day.isoformat()}/part-000.parquet"
    return f"landing/{entity}/dt={day.isoformat()}/part-000.parquet"


def _read_sql(paths: list[str]) -> str:
    literals = ", ".join("'" + path.replace("'", "''") + "'" for path in paths)
    return f"read_parquet([{literals}], hive_partitioning=false)"


def _snapshot(path: Path) -> dict[str, object]:
    warehouse = DuckDBWarehouse(path)
    try:
        totals = warehouse.query(
            """
            SELECT COUNT(*) AS fact_rows,
                   COALESCE(SUM(line_amount_centavos), 0) AS cents
            FROM fact_order_lines
            """
        )[0]
        by_day = warehouse.query(
            """
            SELECT CAST(order_date AS VARCHAR) AS order_date,
                   COUNT(*) AS rows,
                   SUM(line_amount_centavos) AS cents
            FROM fact_order_lines
            GROUP BY 1
            ORDER BY 1
            """
        )
        staged = warehouse.query("SELECT COUNT(*) AS rows FROM stg_order_lines")[0]
    finally:
        warehouse.close()
    return {
        "fact_rows": int(totals["fact_rows"]),
        "cents": int(totals["cents"]),
        "staging_rows": int(staged["rows"]),
        "by_day": [
            {
                "order_date": str(row["order_date"]),
                "rows": int(row["rows"]),
                "cents": int(row["cents"]),
            }
            for row in by_day
        ],
    }


def _write_dim_date() -> int:
    rows = dim_date_rows()
    columns = {
        "date_key": pa.array([row["date_key"] for row in rows], type=pa.int64()),
        "full_date": pa.array([row["full_date"] for row in rows], type=pa.date32()),
        "year": pa.array([row["year"] for row in rows], type=pa.int64()),
        "quarter": pa.array([row["quarter"] for row in rows], type=pa.int64()),
        "month": pa.array([row["month"] for row in rows], type=pa.int64()),
        "day": pa.array([row["day"] for row in rows], type=pa.int64()),
        "day_name": pa.array([row["day_name"] for row in rows], type=pa.string()),
        "is_weekend": pa.array([row["is_weekend"] for row in rows], type=pa.bool_()),
        "is_mx_public_holiday": pa.array(
            [row["is_mx_public_holiday"] for row in rows], type=pa.bool_()
        ),
        "holiday_name": pa.array([row["holiday_name"] for row in rows], type=pa.string()),
    }
    names = column_names(DIM_DATE)
    table = pa.table([columns[name] for name in names], names=names)
    destination = CALENDAR / "dim_date.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, destination, compression="zstd")
    return len(rows)


def _write_correction() -> dict[str, object]:
    source = LANDING / "order_lines" / f"dt={CORRECTION_DAY.isoformat()}" / "part-000.parquet"
    table = pq.read_table(source)
    rows = table.to_pylist()
    updated = next(row for row in rows if row["line_type"] == "sale" and row["quantity"] > 0)
    dropped = rows[-1]
    if dropped["order_line_id"] == updated["order_line_id"]:
        dropped = rows[-2]
    quantity_before = int(updated["quantity"])
    quantity_after = quantity_before + 1
    updated["quantity"] = quantity_after
    updated["line_amount_centavos"] = quantity_after * int(updated["unit_price_centavos"])
    kept = [row for row in rows if row["order_line_id"] != dropped["order_line_id"]]
    destination = (
        CORRECTIONS / "order_lines" / f"dt={CORRECTION_DAY.isoformat()}" / "part-000.parquet"
    )
    destination.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(kept, schema=table.schema), destination, compression="zstd")
    return {
        "date": CORRECTION_DAY.isoformat(),
        "updated_order_line_id": updated["order_line_id"],
        "quantity_before": quantity_before,
        "quantity_after": quantity_after,
        "unit_price_centavos": int(updated["unit_price_centavos"]),
        "dropped_order_line_id": dropped["order_line_id"],
        "rows_before": len(rows),
        "rows_after": len(kept),
    }


def _file_record(path: Path, relative: str) -> dict[str, object]:
    metadata = pq.read_metadata(path)
    return {
        "path": relative,
        "rows": metadata.num_rows,
        "bytes": path.stat().st_size,
    }


def _load_sql(entity: str, days: tuple[date, ...], *, correction: bool) -> list[str]:
    table = f"raw_{entity}"
    predicate = dates_predicate("extract_date", days)
    paths = [_parquet_path(entity, day, correction=correction) for day in days]
    columns = ", ".join(column_names(RAW_BY_ENTITY[entity]))
    return [
        f"DELETE FROM {table} WHERE {predicate}",
        f"INSERT INTO {table} ({columns}) SELECT {columns} FROM {_read_sql(paths)}",
    ]


def _stage_sql(days: tuple[date, ...] | None) -> list[str]:
    return [
        stage_order_lines_sql(CTX, days),
        stage_products_sql(CTX),
        stage_customers_sql(CTX),
        stage_stores_sql(CTX),
        stage_inventory_sql(CTX, days),
    ]


def _publish_incremental_sql(days: tuple[date, ...]) -> list[str]:
    return [
        merge_dimension_sql(CTX, "dim_product", "stg_products", DIM_PRODUCT, "product_id"),
        merge_dimension_sql(CTX, "dim_customer", "stg_customers", DIM_CUSTOMER, "customer_id"),
        merge_dimension_sql(CTX, "dim_store", "stg_stores", DIM_STORE, "store_id"),
        enriched_table_sql(CTX),
        merge_fact_sql(CTX, days),
    ]


def _quality_sql(days: tuple[date, ...] | None, logical: date) -> tuple[list[str], list[str]]:
    checks = build_checks(CTX, fact_dates=days, expected_date=logical)
    return [sql for _, sql in checks], [name for name, _ in checks]


def _resolve_sql(logical: date, lookback: int, days: tuple[date, ...]) -> str:
    return (
        "SELECT "
        f"DATE '{logical.isoformat()}' AS logical_date, "
        f"DATE '{days[0].isoformat()}' AS window_start, "
        f"DATE '{days[-1].isoformat()}' AS window_end, "
        f"{lookback} AS lookback_days"
    )


def _land_sql(entity_paths: dict[str, list[str]]) -> str:
    pieces = []
    for entity, paths in entity_paths.items():
        pieces.append(f"(SELECT '{entity}' AS entity, COUNT(*) AS rows FROM {_read_sql(paths)})")
    return " UNION ALL ".join(pieces) + " ORDER BY entity"


def _boot_sql() -> list[str]:
    columns = ", ".join(column_names(DIM_DATE))
    return [
        *schema_statements(CTX, "US"),
        delete_dim_date_sql(CTX),
        (
            f"INSERT INTO dim_date ({columns}) "
            f"SELECT {columns} FROM {_read_sql(['calendar/dim_date.parquet'])}"
        ),
    ]


def _run_sql(
    logical: date,
    lookback: int,
    *,
    correction: bool,
) -> dict[str, object]:
    days = _window(logical, lookback)
    entity_paths = {
        entity: [_parquet_path(entity, day, correction=correction) for day in days]
        for entity in ENTITIES
    }
    load: list[str] = []
    for entity in ENTITIES:
        load.extend(_load_sql(entity, days, correction=correction))
    load.extend(_stage_sql(days))
    quality_sql, quality_names = _quality_sql(days, logical)
    return {
        "logical_date": logical.isoformat(),
        "lookback_days": lookback,
        "window": [day.isoformat() for day in days],
        "correction": correction,
        "files": entity_paths,
        "tasks": {
            "resolve_window": [_resolve_sql(logical, lookback, days)],
            "generate_and_land": [_land_sql(entity_paths)],
            "load_raw_and_stage": load,
            "publish_marts": _publish_incremental_sql(days),
            "data_quality": quality_sql,
        },
        "quality_names": quality_names,
    }


def _full_refresh_sql() -> dict[str, object]:
    entity_paths = {
        entity: [_parquet_path(entity, day, correction=False) for day in DAYS]
        for entity in ENTITIES
    }
    statements = list(_boot_sql())
    for entity in ENTITIES:
        table = f"raw_{entity}"
        columns = ", ".join(column_names(RAW_BY_ENTITY[entity]))
        paths = entity_paths[entity]
        statements.append(f"DELETE FROM {table}")
        statements.append(
            f"INSERT INTO {table} ({columns}) SELECT {columns} FROM {_read_sql(paths)}"
        )
    statements.extend(_stage_sql(None))
    for table, source, columns in (
        ("dim_product", "stg_products", column_names(DIM_PRODUCT)),
        ("dim_customer", "stg_customers", column_names(DIM_CUSTOMER)),
        ("dim_store", "stg_stores", column_names(DIM_STORE)),
    ):
        chosen = ", ".join(columns)
        statements.append(f"CREATE OR REPLACE TABLE {table} AS\nSELECT {chosen}\nFROM {source}")
    for name in (
        "idx_fact_order_lines_order_date",
        "idx_fact_order_lines_store_id",
        "idx_fact_order_lines_product_id",
    ):
        statements.append(f"DROP INDEX IF EXISTS {name}")
    statements.append(replace_fact_sql(CTX))
    statements.extend(duckdb_index_sqls())
    quality_sql, quality_names = _quality_sql(None, LOGICAL_END)
    return {
        "logical_date": LOGICAL_END.isoformat(),
        "files": entity_paths,
        "statements": statements,
        "quality_sql": quality_sql,
        "quality_names": quality_names,
    }


def _execute_script(connection: duckdb.DuckDBPyConnection, statements: list[str]) -> None:
    for statement in statements:
        connection.execute(statement)


def _measured_snapshot(connection: duckdb.DuckDBPyConnection) -> dict[str, object]:
    totals = connection.execute(
        """
        SELECT COUNT(*) AS fact_rows,
               COALESCE(SUM(line_amount_centavos), 0) AS cents,
               (SELECT COUNT(*) FROM stg_order_lines) AS staging_rows
        FROM fact_order_lines
        """
    ).fetchone()
    assert totals is not None
    by_day = connection.execute(
        """
        SELECT CAST(order_date AS VARCHAR) AS order_date,
               COUNT(*) AS rows,
               SUM(line_amount_centavos) AS cents
        FROM fact_order_lines
        GROUP BY 1
        ORDER BY 1
        """
    ).fetchall()
    return {
        "fact_rows": int(totals[0]),
        "cents": int(totals[1]),
        "staging_rows": int(totals[2]),
        "by_day": [
            {"order_date": str(day), "rows": int(rows), "cents": int(cents)}
            for day, rows, cents in by_day
        ],
    }


def _assert_same(label: str, left: dict[str, object], right: dict[str, object]) -> None:
    if left != right:
        raise SystemExit(f"{label} SQL bundle diverges from run_pipeline:\n{left}\n{right}")


def _quality_total(connection: duckdb.DuckDBPyConnection, statements: list[str]) -> int:
    total = 0
    for statement in statements:
        row = connection.execute(statement).fetchone()
        assert row is not None
        total += int(row[0] or 0)
    return total


def _verify_bundle(runs: dict[str, dict[str, object]], full_refresh: dict[str, object]) -> None:
    connection = duckdb.connect()
    connection.execute(f"SET file_search_path = '{SAMPLE}'")
    _execute_script(connection, _boot_sql())
    for run_id, run in runs.items():
        for task_name in (
            "resolve_window",
            "generate_and_land",
            "load_raw_and_stage",
            "publish_marts",
            "data_quality",
        ):
            _execute_script(connection, run["tasks"][task_name])  # type: ignore[index]
        measured = _measured_snapshot(connection)
        _assert_same(run_id, measured, run["expected"])  # type: ignore[arg-type]
        violations = _quality_total(connection, run["tasks"]["data_quality"])  # type: ignore[index]
        if violations != 0:
            raise SystemExit(f"{run_id} quality violations {violations}")
    fresh = duckdb.connect()
    fresh.execute(f"SET file_search_path = '{SAMPLE}'")
    _execute_script(fresh, full_refresh["statements"])  # type: ignore[arg-type]
    measured = _measured_snapshot(fresh)
    _assert_same("full_refresh", measured, full_refresh["expected"])  # type: ignore[arg-type]
    violations = _quality_total(fresh, full_refresh["quality_sql"])  # type: ignore[arg-type]
    if violations != 0:
        raise SystemExit(f"full refresh quality violations {violations}")


def main() -> None:
    if LANDING.exists():
        shutil.rmtree(LANDING)
    if CORRECTIONS.exists():
        shutil.rmtree(CORRECTIONS)
    CALENDAR.mkdir(parents=True, exist_ok=True)
    dim_rows = _write_dim_date()

    warehouse_dir = SAMPLE / ".build"
    if warehouse_dir.exists():
        shutil.rmtree(warehouse_dir)
    warehouse_dir.mkdir(parents=True)
    settings = _settings(warehouse_dir / "incremental.duckdb")
    generate_days(settings, DAYS[0], DAYS[-1])
    correction = _write_correction()

    runs: dict[str, dict[str, object]] = {}
    run_order: list[str] = []

    def record(run_id: str, logical: date, lookback: int, *, correction_run: bool) -> None:
        report = run_pipeline(
            settings if not correction_run else _settings(warehouse_dir / "incremental.duckdb"),
            mode="incremental",
            logical_date=logical,
            lookback_days=lookback,
            generate=False,
        )
        payload = _run_sql(logical, lookback, correction=correction_run)
        payload["expected"] = _snapshot(warehouse_dir / "incremental.duckdb")
        payload["pipeline"] = {
            "order_line_files_read": report.order_line_files_read,
            "fact_rows_in_window": report.fact_rows_in_window,
            "staging_order_lines": report.staging_order_lines,
            "quality_passed": report.quality_passed,
        }
        runs[run_id] = payload
        run_order.append(run_id)

    try:
        for day in DAYS:
            record(f"inc-{day.isoformat()}", day, 0, correction_run=False)
        record(f"replay-{LOGICAL_END.isoformat()}", LOGICAL_END, 0, correction_run=False)
        record(f"lookback-{LOGICAL_END.isoformat()}", LOGICAL_END, 1, correction_run=False)

        original = LANDING / "order_lines" / f"dt={CORRECTION_DAY.isoformat()}" / "part-000.parquet"
        backup = warehouse_dir / "part-000.parquet"
        shutil.copyfile(original, backup)
        shutil.copyfile(
            CORRECTIONS / "order_lines" / f"dt={CORRECTION_DAY.isoformat()}" / "part-000.parquet",
            original,
        )
        try:
            record(
                f"correction-{CORRECTION_DAY.isoformat()}",
                CORRECTION_DAY,
                0,
                correction_run=True,
            )
        finally:
            shutil.copyfile(backup, original)
    finally:
        shutil.rmtree(warehouse_dir, ignore_errors=True)

    full_settings = _settings(ROOT / "data" / "browser-full.duckdb")
    full_settings.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    if full_settings.duckdb_path.exists():
        full_settings.duckdb_path.unlink()
    full_report = run_pipeline(
        full_settings,
        mode="full_refresh",
        logical_date=LOGICAL_END,
        history_start=DAYS[0],
        generate=False,
    )
    full_expected = _snapshot(full_settings.duckdb_path)
    full_settings.duckdb_path.unlink(missing_ok=True)
    full_refresh = _full_refresh_sql()
    full_refresh["expected"] = full_expected
    full_refresh["pipeline"] = {
        "order_line_files_read": full_report.order_line_files_read,
        "fact_order_lines": full_report.fact_order_lines,
        "quality_passed": full_report.quality_passed,
    }

    # The correction run was measured on the pipeline after swapping the landing
    # file. Re-measure the exported SQL, which reads the corrections path instead.
    _verify_bundle(runs, full_refresh)

    files = []
    for entity in ENTITIES:
        for day in DAYS:
            relative = _parquet_path(entity, day, correction=False)
            files.append(_file_record(SAMPLE / relative, relative))
    correction_relative = _parquet_path("order_lines", CORRECTION_DAY, correction=True)
    files.append(_file_record(SAMPLE / correction_relative, correction_relative))
    files.append(_file_record(CALENDAR / "dim_date.parquet", "calendar/dim_date.parquet"))

    incremental_total = runs[f"inc-{LOGICAL_END.isoformat()}"]["expected"]

    def _fact_identity(snapshot: dict[str, object]) -> dict[str, object]:
        return {
            "fact_rows": snapshot["fact_rows"],
            "cents": snapshot["cents"],
            "by_day": snapshot["by_day"],
        }

    manifest = {
        "generator": {
            "module": "mercadoalba.generate",
            "seed": 42,
            "products": 12,
            "stores": 3,
            "customers": 24,
            "order_lines_per_day": 40,
            "vary_volume_by_weekday": True,
            "start": DAYS[0].isoformat(),
            "end": LOGICAL_END.isoformat(),
        },
        "duckdb_python": duckdb.__version__,
        "dim_date_rows": dim_rows,
        "dim_date_span": ["2024-01-01", "2028-12-31"],
        "correction": correction,
        "files": files,
        "run_order": run_order,
        "full_refresh_matches_incremental_fact": _fact_identity(incremental_total)
        == _fact_identity(full_expected),
        "tasks": [
            "resolve_window",
            "generate_and_land",
            "load_raw_and_stage",
            "publish_marts",
            "data_quality",
        ],
    }
    SAMPLE.mkdir(parents=True, exist_ok=True)
    (SAMPLE / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    bundle = {
        "generated_by": "scripts/export_browser_sample.py",
        "dialect": "duckdb",
        "boot": _boot_sql(),
        "runs": runs,
        "full_refresh": full_refresh,
    }
    (SAMPLE / "sql.json").write_text(json.dumps(bundle, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "runs": run_order,
                "incremental_fact_rows": incremental_total["fact_rows"],
                "incremental_cents": incremental_total["cents"],
                "full_refresh_fact_rows": full_expected["fact_rows"],
                "correction_fact_rows": runs[f"correction-{CORRECTION_DAY.isoformat()}"][
                    "expected"
                ]["fact_rows"],
                "dim_date_rows": dim_rows,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
