"""SQL shared by DuckDB and BigQuery, plus the few statements that cannot be.

Staging, dimension sync, the fact ``MERGE``, and the data-quality queries are
the same text aside from identifier quoting and physical types. Partition and
cluster clauses exist only on the BigQuery DDL. DuckDB gets secondary indexes
that cover the same access paths.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

from mercadoalba.calendar_mx import iter_dates
from mercadoalba.schema import (
    DIM_CUSTOMER,
    DIM_DATE,
    DIM_PRODUCT,
    DIM_STORE,
    FACT_ORDER_LINES,
    RAW_BY_ENTITY,
    STG_INVENTORY,
    Column,
    column_names,
)

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")
_DATASET = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_PROJECT = re.compile(r"^[A-Za-z][A-Za-z0-9-]{0,61}[A-Za-z0-9]$")
_LOCATION = re.compile(r"^[A-Za-z0-9_-]+$")

_SQL_TYPES = {
    ("duckdb", "string"): "VARCHAR",
    ("bigquery", "string"): "STRING",
    ("duckdb", "int64"): "BIGINT",
    ("bigquery", "int64"): "INT64",
    ("duckdb", "date"): "DATE",
    ("bigquery", "date"): "DATE",
    ("duckdb", "timestamp"): "TIMESTAMP",
    ("bigquery", "timestamp"): "TIMESTAMP",
    ("duckdb", "bool"): "BOOLEAN",
    ("bigquery", "bool"): "BOOL",
    ("duckdb", "money"): "DECIMAL(18, 2)",
    ("bigquery", "money"): "NUMERIC",
}


@dataclass(frozen=True)
class SqlContext:
    dialect: str
    project: str | None = None
    dataset: str | None = None

    def __post_init__(self) -> None:
        if self.dialect not in {"duckdb", "bigquery"}:
            raise ValueError("dialect must be 'duckdb' or 'bigquery'")
        if self.dialect == "bigquery":
            if not self.project or not _PROJECT.match(self.project):
                raise ValueError("BigQuery SQL requires a project id")
            if not self.dataset or not _DATASET.match(self.dataset):
                raise ValueError("BigQuery SQL requires a dataset id")

    def table(self, name: str) -> str:
        if not _DATASET.match(name):
            raise ValueError(f"unsafe table name {name}")
        if self.dialect == "bigquery":
            return f"`{self.project}.{self.dataset}.{name}`"
        return name


def sql_type(dialect: str, logical: str) -> str:
    try:
        return _SQL_TYPES[(dialect, logical)]
    except KeyError as exc:
        raise ValueError(f"no SQL type for {dialect}.{logical}") from exc


def money_expr(dialect: str, centavos: str) -> str:
    """Exact pesos from integer centavos. Both engines accept this CAST form."""

    cast = sql_type(dialect, "money")
    return f"CAST({centavos} AS {cast}) / 100"


def dates_predicate(column: str, dates: list[date] | tuple[date, ...]) -> str:
    """A contiguous window becomes ``BETWEEN`` so partition pruning can see it."""

    if not _IDENT.match(column):
        raise ValueError(f"unsafe column reference {column}")
    ordered = sorted(set(dates))
    if not ordered:
        raise ValueError("dates_predicate requires at least one date")
    if ordered == list(iter_dates(ordered[0], ordered[-1])):
        return (
            f"{column} BETWEEN DATE '{ordered[0].isoformat()}' AND DATE '{ordered[-1].isoformat()}'"
        )
    literals = ", ".join(f"DATE '{day.isoformat()}'" for day in ordered)
    return f"{column} IN ({literals})"


def _column_ddl(dialect: str, columns: list[Column]) -> str:
    lines = []
    for name, logical, not_null in columns:
        suffix = " NOT NULL" if not_null else ""
        lines.append(f"  {name} {sql_type(dialect, logical)}{suffix}")
    return ",\n".join(lines)


def create_table_sql(
    ctx: SqlContext,
    name: str,
    columns: list[Column],
    *,
    partition: str | None = None,
    cluster: tuple[str, ...] = (),
) -> str:
    suffix = ""
    if ctx.dialect == "bigquery" and partition:
        if not _DATASET.match(partition):
            raise ValueError(partition)
        suffix += f"\nPARTITION BY {partition}"
    if ctx.dialect == "bigquery" and cluster:
        suffix += "\nCLUSTER BY " + ", ".join(cluster)
    return (
        f"CREATE TABLE IF NOT EXISTS {ctx.table(name)} (\n"
        f"{_column_ddl(ctx.dialect, columns)}\n"
        f"){suffix}"
    )


def create_dataset_sql(ctx: SqlContext, location: str) -> str:
    if ctx.dialect != "bigquery":
        raise ValueError("datasets are a BigQuery concept")
    if not _LOCATION.match(location):
        raise ValueError(f"unsafe location {location}")
    return (
        f"CREATE SCHEMA IF NOT EXISTS `{ctx.project}.{ctx.dataset}` OPTIONS(location='{location}')"
    )


def duckdb_index_sqls() -> list[str]:
    """Secondary indexes that stand in for BigQuery partitioning and clustering.

    BigQuery declares ``PARTITION BY order_date`` and
    ``CLUSTER BY store_id, product_id`` on ``fact_order_lines``. DuckDB has
    no matching table option, so the local warehouse indexes those columns.
    The indexes are an access-path stand-in, not the same storage layout.
    """

    return [
        "CREATE INDEX IF NOT EXISTS idx_fact_order_lines_order_date "
        "ON fact_order_lines(order_date)",
        "CREATE INDEX IF NOT EXISTS idx_fact_order_lines_store_id ON fact_order_lines(store_id)",
        "CREATE INDEX IF NOT EXISTS idx_fact_order_lines_product_id "
        "ON fact_order_lines(product_id)",
        "CREATE INDEX IF NOT EXISTS idx_raw_order_lines_extract_date "
        "ON raw_order_lines(extract_date)",
    ]


def schema_statements(ctx: SqlContext, location: str) -> list[str]:
    statements: list[str] = []
    if ctx.dialect == "bigquery":
        statements.append(create_dataset_sql(ctx, location))
    for entity, columns in RAW_BY_ENTITY.items():
        statements.append(
            create_table_sql(
                ctx,
                f"raw_{entity}",
                columns,
                partition="extract_date",
            )
        )
    statements.append(create_table_sql(ctx, "dim_product", DIM_PRODUCT))
    statements.append(create_table_sql(ctx, "dim_customer", DIM_CUSTOMER))
    statements.append(create_table_sql(ctx, "dim_store", DIM_STORE))
    statements.append(create_table_sql(ctx, "dim_date", DIM_DATE))
    statements.append(
        create_table_sql(
            ctx,
            "fact_order_lines",
            FACT_ORDER_LINES,
            partition="order_date",
            cluster=("store_id", "product_id"),
        )
    )
    if ctx.dialect == "duckdb":
        statements.extend(duckdb_index_sqls())
    return statements


def _select_list(columns: list[Column]) -> str:
    return ",\n      ".join(column_names(columns))


def stage_order_lines_sql(ctx: SqlContext, dates: tuple[date, ...] | None) -> str:
    where = ""
    if dates is not None:
        where = "WHERE " + dates_predicate("order_date", dates)
    amount = money_expr(ctx.dialect, "line_amount_centavos")
    return f"""
CREATE OR REPLACE TABLE {ctx.table("stg_order_lines")} AS
SELECT
      order_line_id,
      order_id,
      customer_id,
      store_id,
      product_id,
      quantity,
      unit_price_centavos,
      line_amount_centavos,
      {amount} AS amount_mxn,
      line_type,
      channel,
      status,
      category,
      order_ts,
      order_date
FROM (
  SELECT
      order_line_id,
      order_id,
      customer_id,
      store_id,
      product_id,
      quantity,
      unit_price_centavos,
      line_amount_centavos,
      line_type,
      channel,
      status,
      category,
      order_ts,
      order_date,
      ROW_NUMBER() OVER (
        PARTITION BY order_line_id
        ORDER BY extract_ts DESC, source_file DESC
      ) AS rn
  FROM {ctx.table("raw_order_lines")}
  {where}
) ranked
WHERE rn = 1
""".strip()


def _latest_snapshot_sql(
    ctx: SqlContext, table: str, staged: str, columns: list[Column], key: str
) -> str:
    selected = _select_list(columns)
    return f"""
CREATE OR REPLACE TABLE {ctx.table(staged)} AS
SELECT
      {selected}
FROM (
  SELECT
      {selected},
      ROW_NUMBER() OVER (
        PARTITION BY {key}
        ORDER BY extract_ts DESC, source_file DESC
      ) AS rn
  FROM {ctx.table(table)}
  WHERE extract_date = (SELECT MAX(extract_date) FROM {ctx.table(table)})
) ranked
WHERE rn = 1
""".strip()


def stage_products_sql(ctx: SqlContext) -> str:
    return _latest_snapshot_sql(ctx, "raw_products", "stg_products", DIM_PRODUCT, "product_id")


def stage_customers_sql(ctx: SqlContext) -> str:
    return _latest_snapshot_sql(ctx, "raw_customers", "stg_customers", DIM_CUSTOMER, "customer_id")


def stage_stores_sql(ctx: SqlContext) -> str:
    return _latest_snapshot_sql(ctx, "raw_stores", "stg_stores", DIM_STORE, "store_id")


def stage_inventory_sql(ctx: SqlContext, dates: tuple[date, ...] | None) -> str:
    where = ""
    if dates is not None:
        where = "WHERE " + dates_predicate("snapshot_date", dates)
    selected = _select_list(STG_INVENTORY)
    return f"""
CREATE OR REPLACE TABLE {ctx.table("stg_daily_inventory")} AS
SELECT
      {selected}
FROM (
  SELECT
      {selected},
      ROW_NUMBER() OVER (
        PARTITION BY store_id, product_id, snapshot_date
        ORDER BY extract_ts DESC, source_file DESC
      ) AS rn
  FROM {ctx.table("raw_daily_inventory")}
  {where}
) ranked
WHERE rn = 1
""".strip()


def enriched_fact_select(ctx: SqlContext) -> str:
    """Star-schema projection. Dropped joins surface as a row-count failure."""

    return f"""
SELECT
      s.order_line_id,
      s.order_id,
      d.date_key,
      p.product_key,
      c.customer_key,
      st.store_key,
      s.quantity,
      s.unit_price_centavos,
      s.line_amount_centavos,
      s.amount_mxn,
      s.line_type,
      s.channel,
      s.status,
      s.category,
      s.order_ts,
      s.order_date,
      s.product_id,
      s.customer_id,
      s.store_id
FROM {ctx.table("stg_order_lines")} AS s
JOIN {ctx.table("dim_product")} AS p
  ON s.product_id = p.product_id
JOIN {ctx.table("dim_customer")} AS c
  ON s.customer_id = c.customer_id
JOIN {ctx.table("dim_store")} AS st
  ON s.store_id = st.store_id
JOIN {ctx.table("dim_date")} AS d
  ON s.order_date = d.full_date
""".strip()


def replace_fact_sql(ctx: SqlContext) -> str:
    """Naive full refresh: rewrite the whole fact, including BigQuery layout."""

    select_sql = enriched_fact_select(ctx)
    if ctx.dialect == "bigquery":
        return (
            f"CREATE OR REPLACE TABLE {ctx.table('fact_order_lines')}\n"
            "PARTITION BY order_date\n"
            "CLUSTER BY store_id, product_id\n"
            "AS\n"
            f"{select_sql}"
        )
    return f"CREATE OR REPLACE TABLE {ctx.table('fact_order_lines')} AS\n{select_sql}"


def _merge_sql(
    ctx: SqlContext,
    target: str,
    source: str,
    keys: tuple[str, ...],
    columns: list[Column],
    *,
    extra_on: str = "",
    not_matched_by_source: str = "TRUE",
) -> str:
    names = column_names(columns)
    update_cols = [name for name in names if name not in keys]
    assignments = ",\n  ".join(f"{name} = src.{name}" for name in update_cols)
    insert_cols = ", ".join(names)
    insert_vals = ", ".join(f"src.{name}" for name in names)
    match = " AND ".join(f"t.{key} = src.{key}" for key in keys)
    if extra_on:
        match = f"{match}\n  AND {extra_on}"
    return f"""
MERGE INTO {ctx.table(target)} AS t
USING {ctx.table(source)} AS src
ON {match}
WHEN MATCHED THEN UPDATE SET
  {assignments}
WHEN NOT MATCHED THEN INSERT ({insert_cols})
VALUES ({insert_vals})
WHEN NOT MATCHED BY SOURCE AND {not_matched_by_source} THEN DELETE
""".strip()


def merge_dimension_sql(
    ctx: SqlContext, target: str, source: str, columns: list[Column], key: str
) -> str:
    return _merge_sql(ctx, target, source, (key,), columns)


def merge_fact_sql(ctx: SqlContext, dates: tuple[date, ...]) -> str:
    """Partition-scoped MERGE.

    The ``order_date`` predicate is on both the match and the
    ``WHEN NOT MATCHED BY SOURCE`` delete. BigQuery can prune
    ``PARTITION BY order_date`` with that predicate. Rows outside the
    window are left alone. Inside the window, changed rows update, new rows
    insert, and rows missing from the extract are deleted.
    """

    predicate = dates_predicate("t.order_date", dates)
    return _merge_sql(
        ctx,
        "fact_order_lines",
        "int_order_line_enriched",
        ("order_line_id", "order_date"),
        FACT_ORDER_LINES,
        extra_on=predicate,
        not_matched_by_source=predicate,
    )


def enriched_table_sql(ctx: SqlContext) -> str:
    return (
        f"CREATE OR REPLACE TABLE {ctx.table('int_order_line_enriched')} AS\n"
        f"{enriched_fact_select(ctx)}"
    )


def sql_literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    raise TypeError(f"unsupported SQL literal {type(value).__name__}")


def insert_values_sql(
    ctx: SqlContext, table: str, columns: list[str], rows: list[dict[str, object]]
) -> str:
    if not rows:
        raise ValueError("insert_values_sql requires rows")
    for name in columns:
        if not _DATASET.match(name):
            raise ValueError(name)
    values = []
    for row in rows:
        values.append("(" + ", ".join(sql_literal(row[name]) for name in columns) + ")")
    column_sql = ", ".join(columns)
    return f"INSERT INTO {ctx.table(table)} ({column_sql})\nVALUES\n  " + ",\n  ".join(values)


def delete_dim_date_sql(ctx: SqlContext) -> str:
    return f"DELETE FROM {ctx.table('dim_date')} WHERE full_date >= DATE '1900-01-01'"
