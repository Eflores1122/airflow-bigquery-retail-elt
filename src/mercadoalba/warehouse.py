"""Warehouse ports.

DuckDB is the local and CI backend. BigQuery is the production-shaped target:
partitioned, clustered fact loads and the same merge SQL. The BigQuery client
is constructed only when that backend is selected, and the SDK is imported
lazily so tests never need GCP credentials.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Protocol

from mercadoalba.config import Settings
from mercadoalba.schema import RAW_BY_ENTITY, column_names
from mercadoalba.sql_builder import SqlContext, dates_predicate, schema_statements


class Warehouse(Protocol):
    dialect: str

    def execute(self, sql: str) -> None: ...

    def query(self, sql: str) -> list[dict[str, Any]]: ...

    def ensure_schema(self) -> None: ...

    def replace_entity(
        self,
        entity: str,
        files: list[Path],
        dates: list[date] | None,
    ) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class LoadRequest:
    """A parquet load. Real BigQuery jobs and the test double both accept this."""

    uris: tuple[str, ...]
    destination: str
    write_disposition: str
    source_format: str = "PARQUET"


def context_for(settings: Settings, warehouse: Warehouse) -> SqlContext:
    if warehouse.dialect == "bigquery":
        return SqlContext("bigquery", settings.bq_project, settings.bq_dataset)
    return SqlContext("duckdb")


class DuckDBWarehouse:
    """A single-writer DuckDB file. Airflow runs are capped at one active run.

    The session does not install or autoload extensions, so reading local
    parquet does not consult Google application-default credentials.
    """

    dialect = "duckdb"

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._con: Any = None

    def __enter__(self) -> DuckDBWarehouse:
        self._connection()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _connection(self) -> Any:
        if self._con is None:
            import duckdb

            self.path.parent.mkdir(parents=True, exist_ok=True)
            connection = duckdb.connect(str(self.path))
            try:
                # Local parquet only. Leaving autoload on would let DuckDB
                # fetch httpfs and consult cloud credentials for a gs:// path.
                connection.execute("SET TimeZone = 'UTC'")
                connection.execute("SET autoinstall_known_extensions = false")
                connection.execute("SET autoload_known_extensions = false")
            except Exception:
                connection.close()
                raise
            self._con = connection
        return self._con

    def execute(self, sql: str) -> None:
        try:
            self._connection().execute(sql)
        except Exception:
            self.close()
            raise

    def query(self, sql: str) -> list[dict[str, Any]]:
        try:
            cursor = self._connection().execute(sql)
            description = cursor.description or []
            columns = [item[0] for item in description]
            return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
        except Exception:
            self.close()
            raise

    def ensure_schema(self) -> None:
        ctx = SqlContext("duckdb")
        for statement in schema_statements(ctx, "US"):
            self.execute(statement)

    def replace_entity(
        self,
        entity: str,
        files: list[Path],
        dates: list[date] | None,
    ) -> None:
        if entity not in RAW_BY_ENTITY:
            raise ValueError(f"unknown entity {entity}")
        table = f"raw_{entity}"
        columns = ", ".join(column_names(RAW_BY_ENTITY[entity]))
        if dates is None:
            self.execute(f"DELETE FROM {table}")
        elif dates:
            self.execute(f"DELETE FROM {table} WHERE {dates_predicate('extract_date', dates)}")
        if not files:
            return
        literals = ", ".join("'" + str(path).replace("'", "''") + "'" for path in files)
        self.execute(
            f"INSERT INTO {table} ({columns}) SELECT {columns} FROM read_parquet([{literals}])"
        )

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None


class BigQueryWarehouse:
    """BigQuery adapter. Not used by tests or CI.

    Landing URIs are ``gs://`` objects with the same relative layout as the
    local landing zone. Partition loads use the ``table$YYYYMMDD`` decorator,
    which is the BigQuery form of replacing one DuckDB ``extract_date``.
    """

    dialect = "bigquery"

    def __init__(
        self,
        *,
        project: str,
        dataset: str,
        location: str,
        landing_root: Path,
        gcs_landing_prefix: str,
        client: Any = None,
    ) -> None:
        if not gcs_landing_prefix.startswith("gs://"):
            raise ValueError("gcs_landing_prefix must start with gs://")
        self.project = project
        self.dataset = dataset
        self.location = location
        self.landing_root = Path(landing_root)
        self.gcs_landing_prefix = gcs_landing_prefix.rstrip("/")
        self.client = client if client is not None else self._build_client()

    def _build_client(self) -> Any:
        try:
            from google.cloud import bigquery
        except ImportError as exc:
            raise ImportError(
                "BigQuery backend requires the optional bigquery extra: "
                "pip install 'mercadoalba-elt[bigquery]'"
            ) from exc
        return bigquery.Client(project=self.project, location=self.location)

    def __enter__(self) -> BigQueryWarehouse:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def execute(self, sql: str) -> None:
        self.query(sql)

    def query(self, sql: str) -> list[dict[str, Any]]:
        job = self.client.query(sql)
        rows = job.result()
        materialized: list[dict[str, Any]] = []
        for row in rows:
            if isinstance(row, dict):
                materialized.append(row)
            elif hasattr(row, "keys"):
                materialized.append({key: row[key] for key in row})
            else:
                materialized.append(dict(row))
        return materialized

    def ensure_schema(self) -> None:
        ctx = SqlContext("bigquery", self.project, self.dataset)
        for statement in schema_statements(ctx, self.location):
            self.execute(statement)

    def replace_entity(
        self,
        entity: str,
        files: list[Path],
        dates: list[date] | None,
    ) -> None:
        if entity not in RAW_BY_ENTITY:
            raise ValueError(f"unknown entity {entity}")
        table = f"{self.project}.{self.dataset}.raw_{entity}"
        grouped: dict[date, list[Path]] = {}
        for path in files:
            grouped.setdefault(_partition_date(path), []).append(path)
        if dates is None:
            self._load(
                LoadRequest(
                    uris=tuple(self._uri(path) for path in files),
                    destination=table,
                    write_disposition="WRITE_TRUNCATE",
                )
            )
            return
        for day in dates:
            day_files = grouped.get(day, [])
            if not day_files:
                continue
            self._load(
                LoadRequest(
                    uris=tuple(self._uri(path) for path in day_files),
                    destination=f"{table}${day.strftime('%Y%m%d')}",
                    write_disposition="WRITE_TRUNCATE",
                )
            )

    def _uri(self, path: Path) -> str:
        relative = path.resolve().relative_to(self.landing_root.resolve())
        return f"{self.gcs_landing_prefix}/{relative.as_posix()}"

    def _load(self, request: LoadRequest) -> None:
        custom = getattr(self.client, "load_parquet_uris", None)
        if callable(custom):
            custom(request)
            return
        from google.cloud import bigquery

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        self.client.load_table_from_uri(
            list(request.uris),
            request.destination,
            job_config=job_config,
        ).result()

    def close(self) -> None:
        closer = getattr(self.client, "close", None)
        if callable(closer):
            closer()


def _partition_date(path: Path) -> date:
    label = path.parent.name
    if not label.startswith("dt="):
        raise ValueError(f"expected a dt=YYYY-MM-DD directory, got {path}")
    return date.fromisoformat(label.removeprefix("dt="))


def create_warehouse(settings: Settings) -> Warehouse:
    if settings.warehouse_backend == "duckdb":
        return DuckDBWarehouse(settings.duckdb_path)
    if settings.warehouse_backend == "bigquery":
        if not settings.bq_project:
            raise ValueError("MERCADOALBA_BQ_PROJECT is required for the bigquery backend")
        if not settings.gcs_landing_prefix:
            raise ValueError("MERCADOALBA_GCS_LANDING_PREFIX is required for the bigquery backend")
        return BigQueryWarehouse(
            project=settings.bq_project,
            dataset=settings.bq_dataset,
            location=settings.bq_location,
            landing_root=settings.landing_root,
            gcs_landing_prefix=settings.gcs_landing_prefix,
        )
    raise ValueError(f"unknown warehouse backend {settings.warehouse_backend}")
