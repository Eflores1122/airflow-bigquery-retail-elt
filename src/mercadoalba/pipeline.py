"""ELT stages shared by the CLI, the benchmark, and the Airflow tasks.

Each public function is safe to run again for the same logical date:

* landing writes overwrite the date partition
* raw loads delete the dates they replace before inserting
* the full refresh rebuilds marts with ``CREATE OR REPLACE``
* the incremental publish ``MERGE``s the touched dates and deletes rows that
  disappeared inside that window only

A transient warehouse error is retried inside ``load_raw_and_stage`` and
``publish_marts``. The data-quality gate is not retried here; Airflow may still
retry the task, and a deterministic failure will fail again.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from time import perf_counter
from typing import Any

from mercadoalba.calendar_mx import dim_date_rows
from mercadoalba.config import Settings
from mercadoalba.generate import generate_days
from mercadoalba.landing import (
    LandingManifest,
    RunWindow,
    build_manifest,
    coerce_window,
    make_window,
)
from mercadoalba.quality import DataQualityError, QualityReport, run_checks
from mercadoalba.retry import call_with_retries
from mercadoalba.schema import DIM_CUSTOMER, DIM_DATE, DIM_PRODUCT, DIM_STORE, column_names
from mercadoalba.sql_builder import (
    SqlContext,
    dates_predicate,
    delete_dim_date_sql,
    duckdb_index_sqls,
    enriched_table_sql,
    insert_values_sql,
    merge_dimension_sql,
    merge_fact_sql,
    replace_fact_sql,
    stage_customers_sql,
    stage_inventory_sql,
    stage_order_lines_sql,
    stage_products_sql,
    stage_stores_sql,
)
from mercadoalba.warehouse import Warehouse, context_for, create_warehouse

LOGGER = logging.getLogger(__name__)

_DIM_DATE_BATCH = 400


def log_airflow_failure(context: dict[str, Any]) -> None:
    """Airflow ``on_failure_callback``. Logs the task identity and nothing else."""

    dag = context.get("dag")
    task_instance = context.get("task_instance")
    LOGGER.error(
        "task failed dag=%s task=%s",
        getattr(dag, "dag_id", None),
        getattr(task_instance, "task_id", None),
    )


@dataclass(frozen=True)
class RunReport:
    mode: str
    logical_date: str
    window_start: str
    window_end: str
    order_line_files_read: int
    staging_order_lines: int
    fact_order_lines: int
    fact_rows_in_window: int
    dim_products: int
    dim_customers: int
    dim_stores: int
    quality_passed: bool
    timings_seconds: dict[str, float]

    def as_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "logical_date": self.logical_date,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "order_line_files_read": self.order_line_files_read,
            "staging_order_lines": self.staging_order_lines,
            "fact_order_lines": self.fact_order_lines,
            "fact_rows_in_window": self.fact_rows_in_window,
            "dim_products": self.dim_products,
            "dim_customers": self.dim_customers,
            "dim_stores": self.dim_stores,
            "quality_passed": self.quality_passed,
            "timings_seconds": self.timings_seconds,
        }


def window_from_airflow(mode: str, context: dict[str, Any]) -> dict[str, object]:
    """Map an Airflow task context onto a JSON-safe run window.

    The scheduled batch uses ``data_interval_start`` (07:00 UTC, which is
    01:00 in America/Mexico_City). A manual run can pass ``logical_date``.
    """

    params = context.get("params") or {}
    override = str(params.get("logical_date") or "").strip()
    logical = date.fromisoformat(override) if override else context["data_interval_start"].date()
    lookback = int(params.get("lookback_days", 1)) if mode == "incremental" else 0
    generate = bool(params.get("generate_synthetic", True))
    return make_window(
        mode=mode,
        logical_date=logical,
        lookback_days=lookback,
        history_start=None,
        generate=generate,
    ).as_dict()


@contextmanager
def _open(settings: Settings, warehouse: Warehouse | None) -> Iterator[Warehouse]:
    if warehouse is not None:
        yield warehouse
        return
    owned = create_warehouse(settings)
    try:
        yield owned
    finally:
        owned.close()


def _count(warehouse: Warehouse, ctx: SqlContext, table: str) -> int:
    rows = warehouse.query(f"SELECT COUNT(*) AS n FROM {ctx.table(table)}")
    return int(rows[0]["n"])


def _ensure_dim_date(warehouse: Warehouse, ctx: SqlContext) -> None:
    expected = dim_date_rows()
    found = _count(warehouse, ctx, "dim_date")
    if found == len(expected):
        return
    warehouse.execute(delete_dim_date_sql(ctx))
    columns = column_names(DIM_DATE)
    for offset in range(0, len(expected), _DIM_DATE_BATCH):
        warehouse.execute(
            insert_values_sql(ctx, "dim_date", columns, expected[offset : offset + _DIM_DATE_BATCH])
        )


def _load_once(settings: Settings, window: RunWindow, warehouse: Warehouse) -> None:
    warehouse.ensure_schema()
    manifest = build_manifest(settings.landing_root, window)
    ctx = context_for(settings, warehouse)
    if window.mode == "full_refresh":
        scope: list[date] | None = None
        stage_dates = None
    else:
        scope = list(manifest.incremental_dates or ())
        stage_dates = manifest.incremental_dates
    warehouse.replace_entity("order_lines", list(manifest.order_line_files), scope)
    warehouse.replace_entity("products", list(manifest.product_files), scope)
    warehouse.replace_entity("customers", list(manifest.customer_files), scope)
    warehouse.replace_entity("stores", list(manifest.store_files), scope)
    warehouse.replace_entity("daily_inventory", list(manifest.inventory_files), scope)
    warehouse.execute(stage_order_lines_sql(ctx, stage_dates))
    warehouse.execute(stage_products_sql(ctx))
    warehouse.execute(stage_customers_sql(ctx))
    warehouse.execute(stage_stores_sql(ctx))
    warehouse.execute(stage_inventory_sql(ctx, stage_dates))
    LOGGER.info("staged %s mode files=%s", window.mode, manifest.order_line_files_read)


def _drop_fact_indexes(warehouse: Warehouse) -> None:
    for name in (
        "idx_fact_order_lines_order_date",
        "idx_fact_order_lines_store_id",
        "idx_fact_order_lines_product_id",
    ):
        warehouse.execute(f"DROP INDEX IF EXISTS {name}")


def _replace_dimension(
    warehouse: Warehouse,
    ctx: SqlContext,
    table: str,
    source: str,
    columns: list[tuple[str, str, bool]],
) -> None:
    chosen = ", ".join(column_names(columns))
    warehouse.execute(
        f"CREATE OR REPLACE TABLE {ctx.table(table)} AS\nSELECT {chosen}\nFROM {ctx.table(source)}"
    )


def _guard_star_join(warehouse: Warehouse, ctx: SqlContext) -> None:
    """Refuse a merge that would delete a partition because the join dropped rows."""

    warehouse.execute(enriched_table_sql(ctx))
    staged = _count(warehouse, ctx, "stg_order_lines")
    enriched = _count(warehouse, ctx, "int_order_line_enriched")
    if staged != enriched:
        raise RuntimeError(
            f"star join dropped rows (staging={staged}, enriched={enriched}); refusing to publish"
        )
    if staged == 0:
        raise RuntimeError("refusing to publish an empty order-line staging table")


def _publish_once(settings: Settings, window: RunWindow, warehouse: Warehouse) -> None:
    warehouse.ensure_schema()
    ctx = context_for(settings, warehouse)
    manifest = build_manifest(settings.landing_root, window)
    _ensure_dim_date(warehouse, ctx)
    for table, label in (
        ("stg_products", "product snapshot"),
        ("stg_customers", "customer snapshot"),
        ("stg_stores", "store snapshot"),
    ):
        if _count(warehouse, ctx, table) == 0:
            raise RuntimeError(f"refusing to publish an empty {label}")
    if window.mode == "full_refresh":
        _replace_dimension(warehouse, ctx, "dim_product", "stg_products", DIM_PRODUCT)
        _replace_dimension(warehouse, ctx, "dim_customer", "stg_customers", DIM_CUSTOMER)
        _replace_dimension(warehouse, ctx, "dim_store", "stg_stores", DIM_STORE)
        _guard_star_join(warehouse, ctx)
        if ctx.dialect == "duckdb":
            _drop_fact_indexes(warehouse)
        warehouse.execute(replace_fact_sql(ctx))
        if ctx.dialect == "duckdb":
            for statement in duckdb_index_sqls():
                warehouse.execute(statement)
        LOGGER.info("published full refresh of fact_order_lines")
        return
    if not manifest.incremental_dates:
        raise RuntimeError("incremental publish has no dates")
    warehouse.execute(
        merge_dimension_sql(ctx, "dim_product", "stg_products", DIM_PRODUCT, "product_id")
    )
    warehouse.execute(
        merge_dimension_sql(ctx, "dim_customer", "stg_customers", DIM_CUSTOMER, "customer_id")
    )
    warehouse.execute(merge_dimension_sql(ctx, "dim_store", "stg_stores", DIM_STORE, "store_id"))
    _guard_star_join(warehouse, ctx)
    warehouse.execute(merge_fact_sql(ctx, manifest.incremental_dates))
    LOGGER.info(
        "merged fact_order_lines for %s",
        ", ".join(day.isoformat() for day in manifest.incremental_dates),
    )


def generate_and_land(
    settings: Settings,
    window: RunWindow | dict[str, object],
) -> dict[str, object]:
    resolved = coerce_window(window)
    if resolved.generate:
        generate_days(settings, resolved.window_start, resolved.window_end)
        LOGGER.info(
            "landed synthetic partitions %s through %s",
            resolved.window_start.isoformat(),
            resolved.window_end.isoformat(),
        )
    else:
        LOGGER.info("synthetic generation skipped")
    return resolved.as_dict()


def load_raw_and_stage(
    settings: Settings,
    window: RunWindow | dict[str, object],
    warehouse: Warehouse | None = None,
) -> dict[str, object]:
    resolved = coerce_window(window)

    def attempt(active: Warehouse) -> None:
        call_with_retries(
            lambda: _load_once(settings, resolved, active),
            attempts=settings.transform_attempts,
        )

    if warehouse is None:
        with _open(settings, None) as active:
            attempt(active)
    else:
        attempt(warehouse)
    return resolved.as_dict()


def publish_marts(
    settings: Settings,
    window: RunWindow | dict[str, object],
    warehouse: Warehouse | None = None,
) -> dict[str, object]:
    resolved = coerce_window(window)

    def attempt(active: Warehouse) -> None:
        call_with_retries(
            lambda: _publish_once(settings, resolved, active),
            attempts=settings.transform_attempts,
        )

    if warehouse is None:
        with _open(settings, None) as active:
            attempt(active)
    else:
        attempt(warehouse)
    return resolved.as_dict()


def data_quality(
    settings: Settings,
    window: RunWindow | dict[str, object],
    warehouse: Warehouse | None = None,
) -> QualityReport:
    resolved = coerce_window(window)
    manifest = build_manifest(settings.landing_root, resolved)
    with _open(settings, warehouse) as active:
        ctx = context_for(settings, active)
        return run_checks(
            active,
            ctx,
            fact_dates=manifest.incremental_dates,
            expected_date=resolved.logical_date,
        )


def _window_row_count(warehouse: Warehouse, ctx: SqlContext, manifest: LandingManifest) -> int:
    if manifest.incremental_dates is None:
        return _count(warehouse, ctx, "fact_order_lines")
    predicate = dates_predicate("order_date", manifest.incremental_dates)
    rows = warehouse.query(
        f"SELECT COUNT(*) AS n FROM {ctx.table('fact_order_lines')} WHERE {predicate}"
    )
    return int(rows[0]["n"])


def run_pipeline(
    settings: Settings,
    *,
    mode: str,
    logical_date: date,
    lookback_days: int | None = None,
    history_start: date | None = None,
    generate: bool = True,
    warehouse: Warehouse | None = None,
) -> RunReport:
    """Run generate, load, publish, and quality in one process."""

    window = make_window(
        mode=mode,
        logical_date=logical_date,
        lookback_days=settings.lookback_days if lookback_days is None else lookback_days,
        history_start=history_start,
        generate=generate,
    )
    started = perf_counter()
    timings: dict[str, float] = {}
    with _open(settings, warehouse) as active:
        mark = perf_counter()
        generate_and_land(settings, window)
        timings["generate"] = perf_counter() - mark

        mark = perf_counter()
        load_raw_and_stage(settings, window, warehouse=active)
        timings["load_and_stage"] = perf_counter() - mark

        mark = perf_counter()
        publish_marts(settings, window, warehouse=active)
        timings["publish"] = perf_counter() - mark

        mark = perf_counter()
        quality = data_quality(settings, window, warehouse=active)
        timings["quality"] = perf_counter() - mark
        if not quality.ok:
            raise DataQualityError(quality)

        ctx = context_for(settings, active)
        manifest = build_manifest(settings.landing_root, window)
        counts = RunReport(
            mode=window.mode,
            logical_date=window.logical_date.isoformat(),
            window_start=window.window_start.isoformat(),
            window_end=window.window_end.isoformat(),
            order_line_files_read=manifest.order_line_files_read,
            staging_order_lines=_count(active, ctx, "stg_order_lines"),
            fact_order_lines=_count(active, ctx, "fact_order_lines"),
            fact_rows_in_window=_window_row_count(active, ctx, manifest),
            dim_products=_count(active, ctx, "dim_product"),
            dim_customers=_count(active, ctx, "dim_customer"),
            dim_stores=_count(active, ctx, "dim_store"),
            quality_passed=True,
            timings_seconds={key: round(value, 6) for key, value in timings.items()},
        )
    total = round(perf_counter() - started, 6)
    timings_with_total = dict(counts.timings_seconds)
    timings_with_total["total"] = total
    LOGGER.info(
        "run complete mode=%s files=%s fact_rows=%s total_s=%s",
        counts.mode,
        counts.order_line_files_read,
        counts.fact_order_lines,
        total,
    )
    return RunReport(
        mode=counts.mode,
        logical_date=counts.logical_date,
        window_start=counts.window_start,
        window_end=counts.window_end,
        order_line_files_read=counts.order_line_files_read,
        staging_order_lines=counts.staging_order_lines,
        fact_order_lines=counts.fact_order_lines,
        fact_rows_in_window=counts.fact_rows_in_window,
        dim_products=counts.dim_products,
        dim_customers=counts.dim_customers,
        dim_stores=counts.dim_stores,
        quality_passed=counts.quality_passed,
        timings_seconds=timings_with_total,
    )
