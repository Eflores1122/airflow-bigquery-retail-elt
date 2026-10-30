"""Runtime settings. Defaults run entirely on local DuckDB."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """Paths, scale, and which warehouse adapter to use.

    ``warehouse_backend`` is ``duckdb`` unless someone explicitly points the
    process at BigQuery. Tests and CI leave it on DuckDB and never need GCP
    credentials.
    """

    warehouse_backend: str = "duckdb"
    duckdb_path: Path = Path("data/warehouse/mercadoalba.duckdb")
    landing_root: Path = Path("data/landing")
    gcs_landing_prefix: str | None = None
    bq_project: str | None = None
    bq_dataset: str = "mercadoalba"
    bq_location: str = "US"
    seed: int = 42
    products: int = 400
    stores: int = 12
    customers: int = 4_000
    order_lines_per_day: int = 5_000
    lookback_days: int = 1
    transform_attempts: int = 3
    vary_volume_by_weekday: bool = True

    def __post_init__(self) -> None:
        if self.warehouse_backend not in {"duckdb", "bigquery"}:
            raise ValueError("warehouse_backend must be 'duckdb' or 'bigquery'")
        if self.products < 1:
            raise ValueError("products must be at least 1")
        if self.stores < 2:
            raise ValueError("stores must be at least 2 so one can be online")
        if self.customers < 1:
            raise ValueError("customers must be at least 1")
        if self.order_lines_per_day < 1:
            raise ValueError("order_lines_per_day must be at least 1")
        if self.lookback_days < 0:
            raise ValueError("lookback_days must be >= 0")
        if self.transform_attempts < 1:
            raise ValueError("transform_attempts must be >= 1")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y"}


def load_settings() -> Settings:
    """Build settings from ``MERCADOALBA_*`` environment variables."""

    return Settings(
        warehouse_backend=os.environ.get("MERCADOALBA_WAREHOUSE", "duckdb").strip().lower(),
        duckdb_path=Path(
            os.environ.get("MERCADOALBA_DUCKDB_PATH", "data/warehouse/mercadoalba.duckdb")
        ),
        landing_root=Path(os.environ.get("MERCADOALBA_LANDING_ROOT", "data/landing")),
        gcs_landing_prefix=os.environ.get("MERCADOALBA_GCS_LANDING_PREFIX") or None,
        bq_project=os.environ.get("MERCADOALBA_BQ_PROJECT") or None,
        bq_dataset=os.environ.get("MERCADOALBA_BQ_DATASET", "mercadoalba"),
        bq_location=os.environ.get("MERCADOALBA_BQ_LOCATION", "US"),
        seed=_env_int("MERCADOALBA_SEED", 42),
        products=_env_int("MERCADOALBA_PRODUCTS", 400),
        stores=_env_int("MERCADOALBA_STORES", 12),
        customers=_env_int("MERCADOALBA_CUSTOMERS", 4_000),
        order_lines_per_day=_env_int("MERCADOALBA_ORDER_LINES_PER_DAY", 5_000),
        lookback_days=_env_int("MERCADOALBA_LOOKBACK_DAYS", 1),
        transform_attempts=_env_int("MERCADOALBA_TRANSFORM_ATTEMPTS", 3),
        vary_volume_by_weekday=_env_bool("MERCADOALBA_VARY_VOLUME_BY_WEEKDAY", True),
    )
