"""Column contracts shared by the generator, the warehouse DDL, and the SQL.

Logical types stay engine-neutral. The warehouse adapter maps them to
DuckDB and BigQuery physical types.
"""

from __future__ import annotations

# name, logical type, not_null
Column = tuple[str, str, bool]

RAW_ORDER_LINES: list[Column] = [
    ("order_line_id", "string", True),
    ("order_id", "string", True),
    ("customer_id", "string", True),
    ("store_id", "string", True),
    ("product_id", "string", True),
    ("quantity", "int64", True),
    ("unit_price_centavos", "int64", True),
    ("line_amount_centavos", "int64", True),
    ("line_type", "string", True),
    ("channel", "string", True),
    ("status", "string", True),
    ("category", "string", True),
    ("order_ts", "timestamp", True),
    ("order_date", "date", True),
    ("extract_ts", "timestamp", True),
    ("extract_date", "date", True),
    ("source_file", "string", True),
]

RAW_PRODUCTS: list[Column] = [
    ("product_key", "int64", True),
    ("product_id", "string", True),
    ("sku", "string", True),
    ("product_name", "string", True),
    ("category", "string", True),
    ("brand", "string", True),
    ("list_price_centavos", "int64", True),
    ("is_active", "bool", True),
    ("extract_ts", "timestamp", True),
    ("extract_date", "date", True),
    ("source_file", "string", True),
]

RAW_CUSTOMERS: list[Column] = [
    ("customer_key", "int64", True),
    ("customer_id", "string", True),
    ("given_name", "string", True),
    ("family_name", "string", True),
    ("city", "string", True),
    ("state_code", "string", True),
    ("state_name", "string", True),
    ("loyalty_tier", "string", True),
    ("email", "string", True),
    ("extract_ts", "timestamp", True),
    ("extract_date", "date", True),
    ("source_file", "string", True),
]

RAW_STORES: list[Column] = [
    ("store_key", "int64", True),
    ("store_id", "string", True),
    ("store_name", "string", True),
    ("channel", "string", True),
    ("city", "string", True),
    ("state_code", "string", True),
    ("state_name", "string", True),
    ("postal_code", "string", True),
    ("opened_on", "date", True),
    ("is_active", "bool", True),
    ("extract_ts", "timestamp", True),
    ("extract_date", "date", True),
    ("source_file", "string", True),
]

RAW_INVENTORY: list[Column] = [
    ("store_id", "string", True),
    ("product_id", "string", True),
    ("snapshot_date", "date", True),
    ("on_hand_units", "int64", True),
    ("reserved_units", "int64", True),
    ("extract_ts", "timestamp", True),
    ("extract_date", "date", True),
    ("source_file", "string", True),
]

RAW_BY_ENTITY: dict[str, list[Column]] = {
    "order_lines": RAW_ORDER_LINES,
    "products": RAW_PRODUCTS,
    "customers": RAW_CUSTOMERS,
    "stores": RAW_STORES,
    "daily_inventory": RAW_INVENTORY,
}

DIM_PRODUCT: list[Column] = [
    ("product_key", "int64", True),
    ("product_id", "string", True),
    ("sku", "string", True),
    ("product_name", "string", True),
    ("category", "string", True),
    ("brand", "string", True),
    ("list_price_centavos", "int64", True),
    ("is_active", "bool", True),
]

DIM_CUSTOMER: list[Column] = [
    ("customer_key", "int64", True),
    ("customer_id", "string", True),
    ("given_name", "string", True),
    ("family_name", "string", True),
    ("city", "string", True),
    ("state_code", "string", True),
    ("state_name", "string", True),
    ("loyalty_tier", "string", True),
    ("email", "string", True),
]

DIM_STORE: list[Column] = [
    ("store_key", "int64", True),
    ("store_id", "string", True),
    ("store_name", "string", True),
    ("channel", "string", True),
    ("city", "string", True),
    ("state_code", "string", True),
    ("state_name", "string", True),
    ("postal_code", "string", True),
    ("opened_on", "date", True),
    ("is_active", "bool", True),
]

DIM_DATE: list[Column] = [
    ("date_key", "int64", True),
    ("full_date", "date", True),
    ("year", "int64", True),
    ("quarter", "int64", True),
    ("month", "int64", True),
    ("day", "int64", True),
    ("day_name", "string", True),
    ("is_weekend", "bool", True),
    ("is_mx_public_holiday", "bool", True),
    ("holiday_name", "string", True),
]

FACT_ORDER_LINES: list[Column] = [
    ("order_line_id", "string", True),
    ("order_id", "string", True),
    ("date_key", "int64", True),
    ("product_key", "int64", True),
    ("customer_key", "int64", True),
    ("store_key", "int64", True),
    ("quantity", "int64", True),
    ("unit_price_centavos", "int64", True),
    ("line_amount_centavos", "int64", True),
    ("amount_mxn", "money", True),
    ("line_type", "string", True),
    ("channel", "string", True),
    ("status", "string", True),
    ("category", "string", True),
    ("order_ts", "timestamp", True),
    ("order_date", "date", True),
    ("product_id", "string", True),
    ("customer_id", "string", True),
    ("store_id", "string", True),
]

STG_INVENTORY: list[Column] = [
    ("store_id", "string", True),
    ("product_id", "string", True),
    ("snapshot_date", "date", True),
    ("on_hand_units", "int64", True),
    ("reserved_units", "int64", True),
]

ENTITIES = ("order_lines", "products", "customers", "stores", "daily_inventory")


def column_names(columns: list[Column]) -> list[str]:
    return [name for name, _, _ in columns]
