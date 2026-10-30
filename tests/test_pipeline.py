"""Behavioral tests for the local DuckDB ELT: parity, backfill, DQ, retries."""

from __future__ import annotations

import shutil
from dataclasses import replace
from datetime import date, timedelta

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from mercadoalba.config import Settings
from mercadoalba.pipeline import (
    data_quality,
    generate_and_land,
    load_raw_and_stage,
    publish_marts,
    run_pipeline,
)
from mercadoalba.warehouse import DuckDBWarehouse


def _settings(tmp_path, **overrides) -> Settings:
    values = {
        "duckdb_path": tmp_path / "warehouse.duckdb",
        "landing_root": tmp_path / "landing",
        "products": 8,
        "stores": 4,
        "customers": 20,
        "order_lines_per_day": 50,
        "lookback_days": 0,
        "transform_attempts": 1,
        "vary_volume_by_weekday": False,
        "seed": 7,
    }
    values.update(overrides)
    return Settings(**values)


def _checksum(path) -> dict:
    warehouse = DuckDBWarehouse(path)
    try:
        rows = warehouse.query(
            """
            SELECT
              COUNT(*) AS n,
              COALESCE(SUM(line_amount_centavos), 0) AS cents,
              COUNT(DISTINCT order_line_id) AS ids,
              COUNT(DISTINCT order_date) AS days
            FROM fact_order_lines
            """
        )
    finally:
        warehouse.close()
    return rows[0]


def _day_sums(path) -> list[tuple]:
    warehouse = DuckDBWarehouse(path)
    try:
        rows = warehouse.query(
            """
            SELECT order_date, COUNT(*) AS n, SUM(line_amount_centavos) AS cents
            FROM fact_order_lines
            GROUP BY order_date
            ORDER BY order_date
            """
        )
    finally:
        warehouse.close()
    return [(row["order_date"], row["n"], row["cents"]) for row in rows]


def test_incremental_matches_full_refresh_and_is_idempotent(tmp_path) -> None:
    start = date(2026, 1, 5)
    end = date(2026, 1, 7)
    full = _settings(tmp_path, duckdb_path=tmp_path / "full.duckdb")
    incremental = replace(full, duckdb_path=tmp_path / "inc.duckdb")
    full_report = run_pipeline(
        full,
        mode="full_refresh",
        logical_date=end,
        history_start=start,
        lookback_days=0,
    )
    assert full_report.order_line_files_read == 3
    assert full_report.fact_order_lines == 150
    assert full_report.dim_stores == 4
    assert full_report.quality_passed

    for offset in range(3):
        report = run_pipeline(
            incremental,
            mode="incremental",
            logical_date=start + timedelta(days=offset),
            generate=False,
            lookback_days=0,
        )
        assert report.order_line_files_read == 1
    assert _checksum(full.duckdb_path) == _checksum(incremental.duckdb_path)
    assert _day_sums(full.duckdb_path) == _day_sums(incremental.duckdb_path)

    again = run_pipeline(
        incremental,
        mode="incremental",
        logical_date=end,
        generate=False,
        lookback_days=0,
    )
    assert again.fact_order_lines == full_report.fact_order_lines
    assert _checksum(incremental.duckdb_path)["ids"] == again.fact_order_lines


def test_backfill_fills_a_missing_day(tmp_path) -> None:
    start = date(2026, 1, 5)
    settings = _settings(tmp_path)
    run_pipeline(
        settings,
        mode="full_refresh",
        logical_date=start + timedelta(days=2),
        history_start=start,
        lookback_days=0,
    )
    expected = _day_sums(settings.duckdb_path)

    gap = replace(settings, duckdb_path=tmp_path / "gap.duckdb")
    run_pipeline(gap, mode="incremental", logical_date=start, generate=False, lookback_days=0)
    run_pipeline(
        gap,
        mode="incremental",
        logical_date=start + timedelta(days=2),
        generate=False,
        lookback_days=0,
    )
    present = {row[0] for row in _day_sums(gap.duckdb_path)}
    assert start + timedelta(days=1) not in present

    run_pipeline(
        gap,
        mode="incremental",
        logical_date=start + timedelta(days=1),
        generate=False,
        lookback_days=0,
    )
    assert _day_sums(gap.duckdb_path) == expected


def test_missing_lookback_partition_does_not_delete_history(tmp_path) -> None:
    day = date(2026, 1, 6)
    prior = day - timedelta(days=1)
    settings = _settings(tmp_path)
    run_pipeline(settings, mode="incremental", logical_date=prior, lookback_days=0)
    run_pipeline(settings, mode="incremental", logical_date=day, lookback_days=0)
    before = _day_sums(settings.duckdb_path)
    shutil.rmtree(settings.landing_root / "order_lines" / f"dt={prior.isoformat()}")
    report = run_pipeline(
        settings,
        mode="incremental",
        logical_date=day,
        generate=False,
        lookback_days=1,
    )
    assert report.order_line_files_read == 1
    assert _day_sums(settings.duckdb_path) == before


def test_merge_updates_and_deletes_inside_the_partition(tmp_path) -> None:
    day = date(2026, 1, 5)
    settings = _settings(tmp_path)
    first = run_pipeline(settings, mode="incremental", logical_date=day, lookback_days=0)
    path = settings.landing_root / "order_lines" / f"dt={day.isoformat()}" / "part-000.parquet"
    table = pq.read_table(path)
    types = table.column("line_type").to_pylist()
    sale = types.index("sale")
    order_line_id = table.column("order_line_id").to_pylist()[sale]
    quantities = table.column("quantity").to_pylist()
    prices = table.column("unit_price_centavos").to_pylist()
    quantities[sale] = int(quantities[sale]) + 1
    updated_amount = int(quantities[sale]) * int(prices[sale])
    amounts = table.column("line_amount_centavos").to_pylist()
    amounts[sale] = updated_amount
    revised = table.set_column(
        table.schema.get_field_index("quantity"),
        "quantity",
        pa.array(quantities, type=table.schema.field("quantity").type),
    )
    revised = revised.set_column(
        revised.schema.get_field_index("line_amount_centavos"),
        "line_amount_centavos",
        pa.array(amounts, type=revised.schema.field("line_amount_centavos").type),
    )
    drop_id = table.column("order_line_id").to_pylist()[0]
    if drop_id == order_line_id:
        drop_id = table.column("order_line_id").to_pylist()[1]
    mask = pc.not_equal(revised.column("order_line_id"), drop_id)
    pq.write_table(revised.filter(mask), path, compression="zstd")

    second = run_pipeline(
        settings,
        mode="incremental",
        logical_date=day,
        generate=False,
        lookback_days=0,
    )
    assert second.fact_order_lines == first.fact_order_lines - 1
    warehouse = DuckDBWarehouse(settings.duckdb_path)
    try:
        rows = warehouse.query(
            f"""
            SELECT line_amount_centavos
            FROM fact_order_lines
            WHERE order_line_id = '{order_line_id}'
            """
        )
        gone = warehouse.query(
            f"SELECT COUNT(*) AS n FROM fact_order_lines WHERE order_line_id = '{drop_id}'"
        )
    finally:
        warehouse.close()
    assert rows[0]["line_amount_centavos"] == updated_amount
    assert gone[0]["n"] == 0


def test_duplicate_extract_keeps_the_latest_row(tmp_path) -> None:
    day = date(2026, 1, 5)
    settings = _settings(tmp_path)
    run_pipeline(settings, mode="incremental", logical_date=day, lookback_days=0)
    path = settings.landing_root / "order_lines" / f"dt={day.isoformat()}" / "part-000.parquet"
    table = pq.read_table(path)
    sale = table.column("line_type").to_pylist().index("sale")
    row = table.slice(sale, 1)
    order_line_id = row.column("order_line_id").to_pylist()[0]
    new_qty = int(row.column("quantity").to_pylist()[0]) + 1
    new_amount = new_qty * int(row.column("unit_price_centavos").to_pylist()[0])
    extract_index = row.schema.get_field_index("extract_ts")
    row = row.set_column(
        row.schema.get_field_index("quantity"),
        "quantity",
        pa.array([new_qty], type=row.schema.field("quantity").type),
    )
    row = row.set_column(
        row.schema.get_field_index("line_amount_centavos"),
        "line_amount_centavos",
        pa.array([new_amount], type=row.schema.field("line_amount_centavos").type),
    )
    later = row.column("extract_ts").to_pylist()[0] + timedelta(hours=1)
    row = row.set_column(
        extract_index,
        "extract_ts",
        pa.array([later], type=row.schema.field("extract_ts").type),
    )
    pq.write_table(row, path.with_name("part-001.parquet"), compression="zstd")

    report = run_pipeline(
        settings,
        mode="incremental",
        logical_date=day,
        generate=False,
        lookback_days=0,
    )
    warehouse = DuckDBWarehouse(settings.duckdb_path)
    try:
        rows = warehouse.query(
            f"""
            SELECT line_amount_centavos
            FROM fact_order_lines
            WHERE order_line_id = '{order_line_id}'
            """
        )
    finally:
        warehouse.close()
    assert report.fact_order_lines == 50
    assert rows[0]["line_amount_centavos"] == new_amount


def test_quality_fails_when_a_product_disappears(tmp_path) -> None:
    day = date(2026, 1, 5)
    settings = _settings(tmp_path)
    run_pipeline(settings, mode="incremental", logical_date=day, lookback_days=0)
    warehouse = DuckDBWarehouse(settings.duckdb_path)
    try:
        warehouse.execute(
            """
            DELETE FROM dim_product
            WHERE product_key = (SELECT MIN(product_key) FROM fact_order_lines)
            """
        )
        report = data_quality(
            settings,
            {
                "mode": "incremental",
                "logical_date": day.isoformat(),
                "window_start": day.isoformat(),
                "window_end": day.isoformat(),
                "generate": False,
            },
            warehouse=warehouse,
        )
    finally:
        warehouse.close()
    assert report.ok is False
    assert any(item.name == "fact_product_fk" and item.violations > 0 for item in report.failed)


def test_airflow_stage_functions_match_a_single_run(tmp_path) -> None:
    day = date(2026, 1, 5)
    single = _settings(tmp_path, duckdb_path=tmp_path / "single.duckdb")
    staged = replace(single, duckdb_path=tmp_path / "staged.duckdb")
    run_pipeline(single, mode="incremental", logical_date=day, lookback_days=0)
    window = {
        "mode": "incremental",
        "logical_date": day.isoformat(),
        "window_start": day.isoformat(),
        "window_end": day.isoformat(),
        "generate": False,
    }
    generate_and_land(staged, window)
    load_raw_and_stage(staged, window)
    publish_marts(staged, window)
    quality = data_quality(staged, window)
    assert quality.ok
    assert _checksum(single.duckdb_path) == _checksum(staged.duckdb_path)


def test_publish_retries_a_transient_merge_error(tmp_path) -> None:
    class FlakyWarehouse(DuckDBWarehouse):
        def __init__(self, path, failures: int) -> None:
            super().__init__(path)
            self.failures = failures

        def execute(self, sql: str) -> None:
            compact = " ".join(sql.split())
            if self.failures > 0 and compact.startswith("MERGE INTO"):
                self.failures -= 1
                raise ConnectionError("simulated transient warehouse error")
            super().execute(sql)

    day = date(2026, 1, 5)
    settings = _settings(tmp_path, transform_attempts=3)
    warehouse = FlakyWarehouse(settings.duckdb_path, failures=2)
    try:
        report = run_pipeline(
            settings,
            mode="incremental",
            logical_date=day,
            lookback_days=0,
            warehouse=warehouse,
        )
    finally:
        warehouse.close()
    assert warehouse.failures == 0
    assert report.quality_passed
    assert report.fact_order_lines == 50


def test_cli_writes_a_local_warehouse(tmp_path) -> None:
    from mercadoalba.__main__ import main

    code = main(
        [
            "--mode",
            "incremental",
            "--date",
            "2026-01-05",
            "--lookback-days",
            "0",
            "--landing-root",
            str(tmp_path / "landing"),
            "--duckdb-path",
            str(tmp_path / "cli.duckdb"),
            "--order-lines-per-day",
            "20",
            "--customers",
            "10",
            "--products",
            "4",
            "--stores",
            "2",
            "--no-weekday-volume",
            "--attempts",
            "1",
        ]
    )
    assert code == 0
    assert (tmp_path / "cli.duckdb").exists()
    assert _checksum(tmp_path / "cli.duckdb")["n"] == 20
