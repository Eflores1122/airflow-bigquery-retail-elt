"""The browser extract and its SQL bundle match the Python pipeline."""

from __future__ import annotations

import json
import shutil
from datetime import date
from pathlib import Path

import duckdb

from mercadoalba.config import Settings
from mercadoalba.pipeline import run_pipeline
from mercadoalba.warehouse import DuckDBWarehouse

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "site" / "public" / "sample"
TASKS = (
    "resolve_window",
    "generate_and_land",
    "load_raw_and_stage",
    "publish_marts",
    "data_quality",
)


def _load() -> tuple[dict, dict]:
    manifest = json.loads((SAMPLE / "manifest.json").read_text(encoding="utf-8"))
    bundle = json.loads((SAMPLE / "sql.json").read_text(encoding="utf-8"))
    return manifest, bundle


def _settings(root: Path, warehouse: Path) -> Settings:
    return Settings(
        duckdb_path=warehouse,
        landing_root=root,
        seed=42,
        products=12,
        stores=3,
        customers=24,
        order_lines_per_day=40,
        lookback_days=0,
        transform_attempts=1,
        vary_volume_by_weekday=True,
    )


def _snapshot(path: Path) -> dict:
    warehouse = DuckDBWarehouse(path)
    try:
        totals = warehouse.query(
            """
            SELECT COUNT(*) AS fact_rows,
                   COALESCE(SUM(line_amount_centavos), 0) AS cents,
                   (SELECT COUNT(*) FROM stg_order_lines) AS staging_rows
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
    finally:
        warehouse.close()
    return {
        "fact_rows": int(totals["fact_rows"]),
        "cents": int(totals["cents"]),
        "staging_rows": int(totals["staging_rows"]),
        "by_day": [
            {
                "order_date": str(row["order_date"]),
                "rows": int(row["rows"]),
                "cents": int(row["cents"]),
            }
            for row in by_day
        ],
    }


def _execute(connection: duckdb.DuckDBPyConnection, statements: list[str]) -> None:
    for statement in statements:
        connection.execute(statement)


def test_exported_sql_matches_recorded_expectations() -> None:
    manifest, bundle = _load()
    assert manifest["full_refresh_matches_incremental_fact"] is True
    assert manifest["dim_date_rows"] == 1827
    assert bundle["dialect"] == "duckdb"
    assert list(bundle["runs"]) == manifest["run_order"]

    connection = duckdb.connect()
    connection.execute(f"SET file_search_path = '{SAMPLE}'")
    _execute(connection, bundle["boot"])
    for run_id in manifest["run_order"]:
        run = bundle["runs"][run_id]
        assert list(run["tasks"]) == list(TASKS)
        merge_sql = run["tasks"]["publish_marts"][-1]
        assert "WHEN NOT MATCHED BY SOURCE AND" in merge_sql
        assert run["window"][0] in merge_sql
        assert run["window"][-1] in merge_sql
        for task in TASKS:
            _execute(connection, run["tasks"][task])
        measured = _snapshot_connection(connection)
        assert measured == run["expected"]
        assert _violations(connection, run["tasks"]["data_quality"]) == 0
        assert len(run["quality_names"]) == len(run["tasks"]["data_quality"])

    fresh = duckdb.connect()
    fresh.execute(f"SET file_search_path = '{SAMPLE}'")
    _execute(fresh, bundle["full_refresh"]["statements"])
    assert _snapshot_connection(fresh) == bundle["full_refresh"]["expected"]
    assert _violations(fresh, bundle["full_refresh"]["quality_sql"]) == 0


def test_pipeline_on_sample_landing_matches_bundle(tmp_path: Path) -> None:
    manifest, bundle = _load()
    landing = tmp_path / "landing"
    shutil.copytree(SAMPLE / "landing", landing)
    warehouse = tmp_path / "warehouse.duckdb"
    settings = _settings(landing, warehouse)

    for day in (
        date(2026, 3, 2),
        date(2026, 3, 3),
        date(2026, 3, 4),
        date(2026, 3, 5),
        date(2026, 3, 6),
    ):
        run_pipeline(
            settings,
            mode="incremental",
            logical_date=day,
            lookback_days=0,
            generate=False,
        )
        assert _snapshot(warehouse) == bundle["runs"][f"inc-{day.isoformat()}"]["expected"]

    run_pipeline(
        settings,
        mode="incremental",
        logical_date=date(2026, 3, 6),
        lookback_days=0,
        generate=False,
    )
    assert _snapshot(warehouse) == bundle["runs"]["replay-2026-03-06"]["expected"]

    run_pipeline(
        settings,
        mode="incremental",
        logical_date=date(2026, 3, 6),
        lookback_days=1,
        generate=False,
    )
    assert _snapshot(warehouse) == bundle["runs"]["lookback-2026-03-06"]["expected"]

    correction = manifest["correction"]
    target = landing / "order_lines" / "dt=2026-03-04" / "part-000.parquet"
    shutil.copyfile(
        SAMPLE / "corrections" / "order_lines" / "dt=2026-03-04" / "part-000.parquet",
        target,
    )
    run_pipeline(
        settings,
        mode="incremental",
        logical_date=date(2026, 3, 4),
        lookback_days=0,
        generate=False,
    )
    measured = _snapshot(warehouse)
    assert measured == bundle["runs"]["correction-2026-03-04"]["expected"]
    assert measured["fact_rows"] == bundle["runs"]["inc-2026-03-06"]["expected"]["fact_rows"] - 1

    updated_id = correction["updated_order_line_id"]
    dropped_id = correction["dropped_order_line_id"]
    warehouse_rows = DuckDBWarehouse(warehouse)
    try:
        found = warehouse_rows.query(
            f"""
            SELECT quantity
            FROM fact_order_lines
            WHERE order_line_id = '{updated_id}'
            """
        )
        missing = warehouse_rows.query(
            f"""
            SELECT COUNT(*) AS n
            FROM fact_order_lines
            WHERE order_line_id = '{dropped_id}'
            """
        )
    finally:
        warehouse_rows.close()
    assert int(found[0]["quantity"]) == correction["quantity_after"]
    assert int(missing[0]["n"]) == 0


def test_full_refresh_on_sample_matches_incremental_fact(tmp_path: Path) -> None:
    _manifest, bundle = _load()
    landing = tmp_path / "landing"
    shutil.copytree(SAMPLE / "landing", landing)
    warehouse = tmp_path / "full.duckdb"
    report = run_pipeline(
        _settings(landing, warehouse),
        mode="full_refresh",
        logical_date=date(2026, 3, 6),
        history_start=date(2026, 3, 2),
        generate=False,
    )
    measured = _snapshot(warehouse)
    expected = bundle["full_refresh"]["expected"]
    assert measured == expected
    incremental = bundle["runs"]["inc-2026-03-06"]["expected"]
    assert measured["fact_rows"] == incremental["fact_rows"]
    assert measured["cents"] == incremental["cents"]
    assert measured["by_day"] == incremental["by_day"]
    assert report.quality_passed is True
    assert report.fact_order_lines == 224


def _snapshot_connection(connection: duckdb.DuckDBPyConnection) -> dict:
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


def _violations(connection: duckdb.DuckDBPyConnection, statements: list[str]) -> int:
    total = 0
    for statement in statements:
        row = connection.execute(statement).fetchone()
        assert row is not None
        total += int(row[0] or 0)
    return total
