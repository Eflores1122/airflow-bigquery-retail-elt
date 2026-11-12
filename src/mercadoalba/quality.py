"""Data-quality gates. Each check returns a ``violations`` count of zero on success.

The same SQL runs on DuckDB and BigQuery. Incremental runs scope row-level fact
checks to the dates just merged. Uniqueness of ``order_line_id`` stays global
so a partition merge cannot hide a duplicate on another day.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Protocol

from mercadoalba.sql_builder import SqlContext, dates_predicate, sql_type


class Queryable(Protocol):
    dialect: str

    def query(self, sql: str) -> list[dict[str, Any]]: ...


@dataclass(frozen=True)
class CheckResult:
    name: str
    violations: int

    def as_dict(self) -> dict[str, int | str]:
        return {"name": self.name, "violations": self.violations}


@dataclass(frozen=True)
class QualityReport:
    results: tuple[CheckResult, ...]

    @property
    def ok(self) -> bool:
        return all(result.violations == 0 for result in self.results)

    @property
    def failed(self) -> tuple[CheckResult, ...]:
        return tuple(result for result in self.results if result.violations)

    def as_dict(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "checks": [result.as_dict() for result in self.results],
        }

    def __str__(self) -> str:
        if self.ok:
            return f"quality passed ({len(self.results)} checks)"
        detail = ", ".join(f"{result.name}={result.violations}" for result in self.failed)
        return f"quality failed: {detail}"


class DataQualityError(RuntimeError):
    """Raised when a gate fails. Retries will not change a deterministic failure."""

    def __init__(self, report: QualityReport) -> None:
        super().__init__(str(report))
        self.report = report


def _where(*parts: str) -> str:
    kept = [part for part in parts if part]
    if not kept:
        return ""
    if len(kept) == 1:
        return f"WHERE {kept[0]}"
    joined = " AND ".join(f"({part})" for part in kept)
    return f"WHERE {joined}"


def _duplicate_groups(table: str, keys: str, predicate: str = "") -> str:
    where = f"WHERE {predicate}" if predicate else ""
    return f"""
SELECT COALESCE(SUM(extra), 0) AS violations
FROM (
  SELECT COUNT(*) - 1 AS extra
  FROM {table}
  {where}
  GROUP BY {keys}
  HAVING COUNT(*) > 1
) dupes
""".strip()


def build_checks(
    ctx: SqlContext,
    *,
    fact_dates: tuple[date, ...] | None,
    expected_date: date,
) -> list[tuple[str, str]]:
    fact = ctx.table("fact_order_lines")
    products = ctx.table("dim_product")
    customers = ctx.table("dim_customer")
    stores = ctx.table("dim_store")
    dates = ctx.table("dim_date")
    staging = ctx.table("stg_order_lines")
    inventory = ctx.table("stg_daily_inventory")
    raw_lines = ctx.table("raw_order_lines")
    raw_inventory = ctx.table("raw_daily_inventory")
    window = "" if fact_dates is None else dates_predicate("order_date", fact_dates)
    window_f = "" if fact_dates is None else dates_predicate("f.order_date", fact_dates)
    fact_window = f"WHERE {window}" if window else ""
    integer = sql_type(ctx.dialect, "int64")
    expected = expected_date.isoformat()
    sign_rule = """
  (line_type = 'sale' AND (quantity <= 0 OR line_amount_centavos <= 0))
  OR (line_type = 'return' AND (quantity >= 0 OR line_amount_centavos >= 0))
  OR line_type NOT IN ('sale', 'return')
  OR status NOT IN ('completed', 'cancelled')
  OR channel NOT IN ('online', 'physical')
""".strip()
    required = """
  order_line_id IS NULL OR order_id IS NULL OR date_key IS NULL
  OR product_key IS NULL OR customer_key IS NULL OR store_key IS NULL
  OR quantity IS NULL OR unit_price_centavos IS NULL OR line_amount_centavos IS NULL
  OR amount_mxn IS NULL OR order_date IS NULL OR product_id IS NULL
  OR customer_id IS NULL OR store_id IS NULL OR line_type IS NULL
  OR status IS NULL OR channel IS NULL OR category IS NULL
""".strip()

    return [
        ("fact_order_line_id_unique", _duplicate_groups(fact, "order_line_id")),
        (
            "fact_required_not_null",
            f"SELECT COUNT(*) AS violations FROM {fact}\n{_where(window, required)}",
        ),
        (
            "fact_line_sign",
            f"SELECT COUNT(*) AS violations FROM {fact}\n{_where(window, sign_rule)}",
        ),
        (
            "fact_line_amount_matches_quantity",
            f"""
SELECT COUNT(*) AS violations
FROM {fact}
{_where(window, "quantity * unit_price_centavos <> line_amount_centavos")}
""".strip(),
        ),
        (
            "fact_amount_mxn_matches_centavos",
            f"""
SELECT COUNT(*) AS violations
FROM {fact}
{_where(window, f"line_amount_centavos <> CAST(ROUND(amount_mxn * 100, 0) AS {integer})")}
""".strip(),
        ),
        (
            "fact_product_fk",
            f"""
SELECT COUNT(*) AS violations
FROM {fact} AS f
LEFT JOIN {products} AS p
  ON f.product_key = p.product_key
 AND f.product_id = p.product_id
{_where("p.product_key IS NULL", window_f)}
""".strip(),
        ),
        (
            "fact_customer_fk",
            f"""
SELECT COUNT(*) AS violations
FROM {fact} AS f
LEFT JOIN {customers} AS c
  ON f.customer_key = c.customer_key
 AND f.customer_id = c.customer_id
{_where("c.customer_key IS NULL", window_f)}
""".strip(),
        ),
        (
            "fact_store_fk",
            f"""
SELECT COUNT(*) AS violations
FROM {fact} AS f
LEFT JOIN {stores} AS st
  ON f.store_key = st.store_key
 AND f.store_id = st.store_id
{_where("st.store_key IS NULL", window_f)}
""".strip(),
        ),
        (
            "fact_date_fk",
            f"""
SELECT COUNT(*) AS violations
FROM {fact} AS f
LEFT JOIN {dates} AS d
  ON f.date_key = d.date_key
 AND f.order_date = d.full_date
{_where("d.date_key IS NULL", window_f)}
""".strip(),
        ),
        (
            "fact_category_matches_product",
            f"""
SELECT COUNT(*) AS violations
FROM {fact} AS f
JOIN {products} AS p
  ON f.product_key = p.product_key
{_where("f.category <> p.category", window_f)}
""".strip(),
        ),
        (
            "fact_channel_matches_store",
            f"""
SELECT COUNT(*) AS violations
FROM {fact} AS f
JOIN {stores} AS st
  ON f.store_key = st.store_key
{_where("f.channel <> st.channel", window_f)}
""".strip(),
        ),
        (
            "staging_matches_fact_rows",
            f"""
SELECT ABS(
  (SELECT COUNT(*) FROM {staging}) -
  (SELECT COUNT(*) FROM {fact} {fact_window})
) AS violations
""".strip(),
        ),
        (
            "fact_freshness",
            f"""
SELECT CASE
  WHEN COALESCE((SELECT MAX(order_date) FROM {fact}), DATE '1900-01-01')
       >= DATE '{expected}' THEN 0
  ELSE 1
END AS violations
""".strip(),
        ),
        ("dim_product_id_unique", _duplicate_groups(products, "product_id")),
        ("dim_customer_id_unique", _duplicate_groups(customers, "customer_id")),
        ("dim_store_id_unique", _duplicate_groups(stores, "store_id")),
        (
            "inventory_not_null_or_negative",
            f"""
SELECT COUNT(*) AS violations
FROM {inventory}
WHERE store_id IS NULL
   OR product_id IS NULL
   OR snapshot_date IS NULL
   OR on_hand_units IS NULL
   OR reserved_units IS NULL
   OR on_hand_units < 0
   OR reserved_units < 0
   OR reserved_units > on_hand_units
""".strip(),
        ),
        (
            "inventory_unique",
            _duplicate_groups(inventory, "store_id, product_id, snapshot_date"),
        ),
        (
            "inventory_store_fk",
            f"""
SELECT COUNT(*) AS violations
FROM {inventory} AS i
LEFT JOIN {stores} AS st
  ON i.store_id = st.store_id
WHERE st.store_id IS NULL
""".strip(),
        ),
        (
            "inventory_product_fk",
            f"""
SELECT COUNT(*) AS violations
FROM {inventory} AS i
LEFT JOIN {products} AS p
  ON i.product_id = p.product_id
WHERE p.product_id IS NULL
""".strip(),
        ),
        (
            "raw_extract_date_matches_order_date",
            f"""
SELECT COUNT(*) AS violations
FROM {raw_lines}
WHERE extract_date <> order_date
""".strip(),
        ),
        (
            "raw_inventory_extract_date_matches_snapshot",
            f"""
SELECT COUNT(*) AS violations
FROM {raw_inventory}
WHERE extract_date <> snapshot_date
""".strip(),
        ),
    ]


def run_checks(
    warehouse: Queryable,
    ctx: SqlContext,
    *,
    fact_dates: tuple[date, ...] | None,
    expected_date: date,
) -> QualityReport:
    results: list[CheckResult] = []
    for name, sql in build_checks(ctx, fact_dates=fact_dates, expected_date=expected_date):
        rows = warehouse.query(sql)
        if not rows or "violations" not in rows[0]:
            raise RuntimeError(f"quality check {name} did not return violations")
        results.append(CheckResult(name=name, violations=int(rows[0]["violations"] or 0)))
    return QualityReport(results=tuple(results))
