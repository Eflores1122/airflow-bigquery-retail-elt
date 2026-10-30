"""Put the package on the path and keep Airflow metadata inside the workspace."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

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
