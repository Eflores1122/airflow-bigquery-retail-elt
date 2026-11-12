"""Generator, calendar, SQL contract, retries, and the BigQuery adapter double."""

from __future__ import annotations

import builtins
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from mercadoalba.calendar_mx import dim_date_rows, holiday_name
from mercadoalba.config import Settings, load_settings
from mercadoalba.generate import build_population, generate_days, order_lines_for_day
from mercadoalba.pipeline import window_from_airflow
from mercadoalba.retry import call_with_retries
from mercadoalba.sql_builder import (
    SqlContext,
    dates_predicate,
    merge_fact_sql,
    schema_statements,
    stage_order_lines_sql,
)
from mercadoalba.warehouse import BigQueryWarehouse, LoadRequest, create_warehouse

ROOT = Path(__file__).resolve().parents[1]


def test_mexican_public_holidays_use_observed_mondays() -> None:
    assert holiday_name(date(2026, 1, 1)) == "Año Nuevo"
    assert holiday_name(date(2026, 2, 2)) == "Día de la Constitución"
    assert holiday_name(date(2026, 3, 16)) == "Natalicio de Benito Juárez"
    assert holiday_name(date(2026, 5, 1)) == "Día del Trabajo"
    assert holiday_name(date(2026, 9, 16)) == "Día de la Independencia"
    assert holiday_name(date(2026, 11, 16)) == "Día de la Revolución"
    assert holiday_name(date(2026, 12, 25)) == "Navidad"
    assert holiday_name(date(2026, 6, 10)) is None

    rows = {row["full_date"]: row for row in dim_date_rows()}
    assert rows[date(2026, 1, 1)]["is_mx_public_holiday"] is True
    assert rows[date(2026, 1, 1)]["date_key"] == 20260101
    assert rows[date(2026, 6, 10)]["day_name"] == "Wednesday"
    assert rows[date(2026, 6, 13)]["is_weekend"] is True


def test_population_is_synthetic_and_referential(tmp_path) -> None:
    settings = Settings(
        landing_root=tmp_path / "landing",
        duckdb_path=tmp_path / "unused.duckdb",
        products=6,
        stores=3,
        customers=12,
        order_lines_per_day=30,
        vary_volume_by_weekday=False,
        seed=7,
    )
    population = build_population(settings)
    assert all(
        name.startswith("Producto Sintetico") for name in population.products["product_name"]
    )
    assert all(name.startswith("Sintetico") for name in population.customers["family_name"])
    assert population.stores["store_name"][0] == "Mercado Alba Digital"
    assert population.stores["channel"][0] == "online"
    assert set(population.stores["channel"]) == {"online", "physical"}

    stats = generate_days(settings, date(2026, 1, 5), date(2026, 1, 5))
    assert stats[0].order_lines == 30
    table = pq.read_table(tmp_path / "landing/order_lines/dt=2026-01-05/part-000.parquet")
    assert set(table.column("order_date").to_pylist()) == {date(2026, 1, 5)}
    product_ids = set(population.products["product_id"])
    assert set(table.column("product_id").to_pylist()) <= product_ids
    assert "online" in set(table.column("channel").to_pylist())
    assert "physical" in set(table.column("channel").to_pylist())


def test_weekday_volume_curve() -> None:
    monday = date(2026, 1, 5)
    sunday = date(2026, 1, 11)
    assert monday.weekday() == 0
    assert sunday.weekday() == 6
    assert order_lines_for_day(monday, 100, True) > order_lines_for_day(sunday, 100, True)
    assert order_lines_for_day(sunday, 100, False) == 100


def test_sql_contract_shares_merge_and_splits_ddl() -> None:
    duck = SqlContext("duckdb")
    bigquery = SqlContext("bigquery", project="demo-mercadoalba", dataset="mercadoalba")
    duck_ddl = "\n".join(schema_statements(duck, "US"))
    bq_ddl = "\n".join(schema_statements(bigquery, "US"))
    assert "PARTITION BY" not in duck_ddl
    assert "CREATE INDEX IF NOT EXISTS idx_fact_order_lines_store_id" in duck_ddl
    assert "PARTITION BY order_date" in bq_ddl
    assert "CLUSTER BY store_id, product_id" in bq_ddl
    assert "CREATE SCHEMA IF NOT EXISTS `demo-mercadoalba.mercadoalba`" in bq_ddl

    window = (date(2026, 1, 5), date(2026, 1, 6))
    for ctx in (duck, bigquery):
        merge = merge_fact_sql(ctx, window)
        assert "MERGE INTO" in merge
        assert "WHEN MATCHED THEN UPDATE" in merge
        assert "WHEN NOT MATCHED THEN INSERT" in merge
        assert "WHEN NOT MATCHED BY SOURCE AND" in merge
        assert "BETWEEN DATE '2026-01-05' AND DATE '2026-01-06'" in merge
    assert "`demo-mercadoalba.mercadoalba.fact_order_lines`" in merge_fact_sql(bigquery, window)

    full_stage = stage_order_lines_sql(duck, None)
    incremental_stage = stage_order_lines_sql(duck, (date(2026, 1, 6),))
    assert "BETWEEN" not in full_stage
    assert "DATE '2026-01-06'" in incremental_stage


def test_dates_predicate_uses_between_for_a_contiguous_window() -> None:
    assert dates_predicate("order_date", [date(2026, 1, 6), date(2026, 1, 5)]) == (
        "order_date BETWEEN DATE '2026-01-05' AND DATE '2026-01-06'"
    )
    gapped = dates_predicate("order_date", [date(2026, 1, 5), date(2026, 1, 7)])
    assert gapped.startswith("order_date IN (")
    assert "2026-01-06" not in gapped


def test_retries_stop_after_success_and_reraise() -> None:
    calls = {"n": 0}

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("transient")
        return "ok"

    assert call_with_retries(flaky, attempts=3) == "ok"
    assert calls["n"] == 3

    def always() -> None:
        raise ConnectionError("nope")

    with pytest.raises(ConnectionError):
        call_with_retries(always, attempts=2)


def test_unset_warehouse_stays_on_duckdb_when_gcp_env_is_present(monkeypatch) -> None:
    monkeypatch.delenv("MERCADOALBA_WAREHOUSE", raising=False)
    monkeypatch.delenv("MERCADOALBA_BQ_PROJECT", raising=False)
    monkeypatch.delenv("MERCADOALBA_GCS_LANDING_PREFIX", raising=False)
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/does-not-exist-adc.json")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "not-a-real-project")
    settings = load_settings()
    assert settings.warehouse_backend == "duckdb"
    assert settings.bq_project is None
    assert settings.gcs_landing_prefix is None


def test_settings_from_environment(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MERCADOALBA_WAREHOUSE", "duckdb")
    monkeypatch.setenv("MERCADOALBA_LANDING_ROOT", str(tmp_path / "land"))
    monkeypatch.setenv("MERCADOALBA_ORDER_LINES_PER_DAY", "12")
    monkeypatch.setenv("MERCADOALBA_VARY_VOLUME_BY_WEEKDAY", "false")
    settings = load_settings()
    assert settings.landing_root == tmp_path / "land"
    assert settings.order_lines_per_day == 12
    assert settings.vary_volume_by_weekday is False


def test_bigquery_load_uses_partition_decorator(tmp_path) -> None:
    class FakeJob:
        def result(self) -> list:
            return []

    class FakeClient:
        def __init__(self) -> None:
            self.queries: list[str] = []
            self.loads: list[LoadRequest] = []

        def query(self, sql: str) -> FakeJob:
            self.queries.append(sql)
            return FakeJob()

        def load_parquet_uris(self, request: LoadRequest) -> None:
            self.loads.append(request)

    landing = tmp_path / "landing"
    day = date(2026, 1, 5)
    path = landing / "order_lines" / f"dt={day.isoformat()}" / "part-000.parquet"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"")
    client = FakeClient()
    warehouse = BigQueryWarehouse(
        project="demo-mercadoalba",
        dataset="mercadoalba",
        location="US",
        landing_root=landing,
        gcs_landing_prefix="gs://demo-bucket/mercadoalba",
        client=client,
    )
    warehouse.ensure_schema()
    fact_ddl = [sql for sql in client.queries if "fact_order_lines" in sql]
    assert any("PARTITION BY order_date" in sql for sql in fact_ddl)
    assert any("CLUSTER BY store_id, product_id" in sql for sql in fact_ddl)

    warehouse.replace_entity("order_lines", [path], [day])
    warehouse.replace_entity("order_lines", [path], None)
    assert client.loads[0].destination == "demo-mercadoalba.mercadoalba.raw_order_lines$20260105"
    assert client.loads[0].write_disposition == "WRITE_TRUNCATE"
    assert client.loads[0].uris == (
        "gs://demo-bucket/mercadoalba/order_lines/dt=2026-01-05/part-000.parquet",
    )
    assert client.loads[1].destination == "demo-mercadoalba.mercadoalba.raw_order_lines"
    assert "$" not in client.loads[1].destination


def test_bigquery_backend_requires_the_optional_extra(monkeypatch, tmp_path) -> None:
    real_import = builtins.__import__

    def guarded(name, global_ns=None, local_ns=None, fromlist=(), level=0):
        if name == "google.cloud" or name.startswith("google.cloud"):
            raise ImportError("blocked for test")
        return real_import(name, global_ns, local_ns, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", guarded)
    with pytest.raises(ImportError, match="bigquery extra"):
        BigQueryWarehouse(
            project="demo-mercadoalba",
            dataset="mercadoalba",
            location="US",
            landing_root=tmp_path,
            gcs_landing_prefix="gs://demo-bucket/mercadoalba",
        )


def test_factory_rejects_bigquery_without_a_project(tmp_path) -> None:
    settings = Settings(
        warehouse_backend="bigquery",
        landing_root=tmp_path,
        duckdb_path=tmp_path / "unused.duckdb",
    )
    with pytest.raises(ValueError, match="BQ_PROJECT"):
        create_warehouse(settings)


def test_readme_states_the_reference_boundary() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8").lower()
    assert "edgar flores" in readme
    assert "fictitious" in readme
    assert "synthetic" in readme
    assert "mercado alba" in readme
    assert "reference implementation" in readme
    assert "local synthetic" in readme
    assert "incremental" in readme
    assert "backfill" in readme
    assert "## tech stack" in readme
    assert "## limitations" in readme
    assert "pip install -e ." in readme
    assert '"fact_order_lines": 4000' in readme
    assert '"dim_products": 40' in readme
    assert '"dim_customers": 500' in readme
    assert '"dim_stores": 4' in readme
    assert '"quality_passed": true' in readme


def test_incremental_window_from_a_fake_airflow_context() -> None:
    class Interval:
        def date(self) -> date:
            return date(2026, 4, 2)

    payload = window_from_airflow(
        "incremental",
        {
            "params": {"logical_date": "", "lookback_days": 1, "generate_synthetic": False},
            "data_interval_start": Interval(),
        },
    )
    assert payload["logical_date"] == "2026-04-02"
    assert payload["window_start"] == "2026-04-01"
    assert payload["generate"] is False

    overridden = window_from_airflow(
        "full_refresh",
        {
            "params": {"logical_date": "2026-05-01", "generate_synthetic": True},
            "data_interval_start": Interval(),
        },
    )
    assert overridden["logical_date"] == "2026-05-01"
    assert overridden["window_start"] == "2026-05-01"
    assert overridden["mode"] == "full_refresh"
