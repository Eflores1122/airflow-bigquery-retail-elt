"""Full-refresh ELT for Mercado Alba, a fictitious Mexican retailer.

Reference implementation built with synthetic data. Each run rewrites the
star schema from every order-line partition in the landing zone.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

from mercadoalba.config import load_settings
from mercadoalba.pipeline import (
    data_quality,
    generate_and_land,
    load_raw_and_stage,
    log_airflow_failure,
    publish_marts,
    window_from_airflow,
)

_DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
    "sla": timedelta(hours=2),
    "on_failure_callback": log_airflow_failure,
}


@dag(
    dag_id="mercadoalba_elt_full_refresh",
    description="Naive full refresh of the synthetic Mercado Alba star schema.",
    schedule="0 7 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=3),
    default_args=_DEFAULT_ARGS,
    params={
        "logical_date": Param(
            "",
            type="string",
            description="Optional YYYY-MM-DD override. Empty uses the data interval start.",
        ),
        "generate_synthetic": Param(True, type="boolean"),
    },
    tags=["mercadoalba", "elt", "synthetic", "full-refresh"],
    doc_md=__doc__,
)
def mercadoalba_elt_full_refresh():
    @task(
        task_id="resolve_window",
        retries=1,
        execution_timeout=timedelta(minutes=5),
        sla=timedelta(minutes=10),
    )
    def resolve_window() -> dict:
        return window_from_airflow("full_refresh", get_current_context())

    @task(
        task_id="generate_and_land",
        execution_timeout=timedelta(minutes=30),
        sla=timedelta(minutes=20),
    )
    def generate_and_land_task(window: dict) -> dict:
        return generate_and_land(load_settings(), window)

    @task(
        task_id="load_raw_and_stage",
        execution_timeout=timedelta(hours=2),
        sla=timedelta(hours=2),
    )
    def load_raw_and_stage_task(window: dict) -> dict:
        return load_raw_and_stage(load_settings(), window)

    @task(
        task_id="publish_marts",
        execution_timeout=timedelta(hours=2),
        sla=timedelta(hours=2),
    )
    def publish_marts_task(window: dict) -> dict:
        return publish_marts(load_settings(), window)

    @task(
        task_id="data_quality",
        retries=1,
        execution_timeout=timedelta(minutes=30),
        sla=timedelta(minutes=20),
    )
    def data_quality_task(window: dict) -> None:
        report = data_quality(load_settings(), window)
        if not report.ok:
            raise RuntimeError(str(report))

    data_quality_task(
        publish_marts_task(load_raw_and_stage_task(generate_and_land_task(resolve_window())))
    )


mercadoalba_elt_full_refresh()
