"""Parse the TaskFlow DAGs without a scheduler, a database migration, or GCP."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
DAG_IDS = ("mercadoalba_elt_full_refresh", "mercadoalba_elt_incremental")
TASK_ORDER = (
    "resolve_window",
    "generate_and_land",
    "load_raw_and_stage",
    "publish_marts",
    "data_quality",
)


@pytest.fixture(scope="module")
def dag_bag():
    from airflow.models import DagBag

    bag = DagBag(dag_folder=str(ROOT / "dags"), include_examples=False)
    return bag


def test_dags_import(dag_bag) -> None:
    assert dag_bag.import_errors == {}
    assert set(DAG_IDS) <= set(dag_bag.dags)


@pytest.mark.parametrize("dag_id", DAG_IDS)
def test_elt_tasks_retries_and_sla(dag_bag, dag_id: str) -> None:
    dag = dag_bag.dags[dag_id]
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.dagrun_timeout is not None
    assert "synthetic" in dag.tags
    assert dag.doc_md
    assert "fictitious" in dag.doc_md.lower() or "synthetic" in dag.doc_md.lower()
    schedule = getattr(dag, "schedule_interval", None)
    assert schedule == "0 7 * * *" or str(schedule) == "0 7 * * *"

    pairs = zip(TASK_ORDER, TASK_ORDER[1:], strict=False)
    for upstream, downstream in pairs:
        assert upstream in dag.get_task(downstream).upstream_task_ids

    publish = dag.get_task("publish_marts")
    quality = dag.get_task("data_quality")
    assert publish.retries == 2
    assert publish.retry_exponential_backoff is True
    assert publish.sla is not None
    assert publish.on_failure_callback
    assert quality.retries == 1
    assert quality.owner == "data-engineering"
    assert dag.dagrun_timeout >= timedelta(hours=1)
