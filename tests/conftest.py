"""Put the package on the path and keep Airflow metadata inside the workspace."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# The suite runs the local DuckDB path. Drop ambient GCP settings so a
# machine with application-default credentials cannot redirect tests at
# BigQuery or fail on a credential file.
for _gcp_name in (
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GOOGLE_CLOUD_PROJECT",
    "GCLOUD_PROJECT",
    "CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE",
    "CLOUDSDK_AUTH_ACCESS_TOKEN",
    "GOOGLE_GHA_CREDS_PATH",
    "MERCADOALBA_BQ_PROJECT",
    "MERCADOALBA_GCS_LANDING_PREFIX",
):
    os.environ.pop(_gcp_name, None)
os.environ["MERCADOALBA_WAREHOUSE"] = "duckdb"

AIRFLOW_HOME = ROOT / ".airflow-tests"
AIRFLOW_HOME.mkdir(exist_ok=True)
os.environ["AIRFLOW_HOME"] = str(AIRFLOW_HOME)
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ["AIRFLOW__CORE__EXECUTOR"] = "SequentialExecutor"
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(ROOT / "dags")
os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{AIRFLOW_HOME / 'airflow.db'}"
os.environ["AIRFLOW__CORE__FERNET_KEY"] = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
os.environ["AIRFLOW__WEBSERVER__SECRET_KEY"] = "mercadoalba-test-secret"
