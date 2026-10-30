"""Deterministic synthetic extracts for the fictitious Mercado Alba ledger.

Mercado Alba is an invented Mexican omnichannel retailer. Every person,
product, and store name is generated. Re-running a date with the same seed
overwrites the same parquet partition, which is what makes landing idempotent.

Timestamps in the files are naive UTC. ``order_date`` and ``snapshot_date``
are the civil date in America/Mexico_City (UTC-6, no daylight saving in this
range).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from mercadoalba.calendar_mx import iter_dates
from mercadoalba.config import Settings
from mercadoalba.landing import partition_path, source_file_name
from mercadoalba.schema import (
    RAW_CUSTOMERS,
    RAW_INVENTORY,
    RAW_ORDER_LINES,
    RAW_PRODUCTS,
    RAW_STORES,
    Column,
    column_names,
)

MX = ZoneInfo("America/Mexico_City")

GIVEN_NAMES = (
    "Ana",
    "Luis",
    "María",
    "José",
    "Sofía",
    "Diego",
    "Camila",
    "Santiago",
    "Regina",
    "Emilio",
    "Ximena",
    "Iker",
    "Renata",
    "Gael",
    "Natalia",
    "Leo",
    "Fernanda",
    "Emiliano",
)

CITIES = (
    ("Ciudad de México", "CMX", "Ciudad de México"),
    ("Guadalajara", "JAL", "Jalisco"),
    ("Monterrey", "NLE", "Nuevo León"),
    ("Puebla", "PUE", "Puebla"),
    ("Querétaro", "QUE", "Querétaro"),
    ("Mérida", "YUC", "Yucatán"),
    ("León", "GUA", "Guanajuato"),
    ("Toluca", "MEX", "Estado de México"),
    ("Cancún", "ROO", "Quintana Roo"),
    ("Tijuana", "BCN", "Baja California"),
)

CATEGORIES = (
    ("abarrotes", "Alba Basicos"),
    ("electronica", "Alba Circuito"),
    ("ropa", "Alba Textil"),
    ("hogar", "Alba Casa"),
    ("belleza", "Alba Cuidado"),
    ("juguetes", "Alba Juego"),
)

LOYALTY = ("bronce", "plata", "oro")
WEEKDAY_FACTORS = (1.05, 1.10, 1.10, 1.15, 1.20, 0.70, 0.45)


@dataclass(frozen=True)
class Population:
    products: dict[str, list]
    customers: dict[str, list]
    stores: dict[str, list]


@dataclass(frozen=True)
class DayStats:
    day: date
    order_lines: int
    products: int
    customers: int
    stores: int
    inventory_rows: int


def order_lines_for_day(day: date, base: int, vary: bool) -> int:
    if not vary:
        return base
    return max(1, int(round(base * WEEKDAY_FACTORS[day.weekday()])))


def _rng(*parts: int) -> np.random.Generator:
    return np.random.default_rng(np.random.SeedSequence(list(parts)))


def _arrow_type(logical: str) -> pa.DataType:
    return {
        "string": pa.string(),
        "int64": pa.int64(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("us"),
        "bool": pa.bool_(),
    }[logical]


def _table(columns: list[Column], data: dict[str, list]) -> pa.Table:
    arrays = []
    fields = []
    for name, logical, not_null in columns:
        arrays.append(pa.array(data[name], type=_arrow_type(logical)))
        fields.append(pa.field(name, _arrow_type(logical), nullable=not not_null))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def write_parquet(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    pq.write_table(table, temporary, compression="zstd")
    temporary.replace(path)


def build_population(settings: Settings) -> Population:
    """Stable catalog, shoppers, and stores. Attributes do not change by day."""

    rng = _rng(settings.seed, 17)
    product_count = settings.products
    categories = [CATEGORIES[i % len(CATEGORIES)] for i in range(product_count)]
    products = {
        "product_key": list(range(1, product_count + 1)),
        "product_id": [f"P{i:06d}" for i in range(1, product_count + 1)],
        "sku": [f"SKU{i:06d}" for i in range(1, product_count + 1)],
        "product_name": [f"Producto Sintetico {i:05d}" for i in range(1, product_count + 1)],
        "category": [item[0] for item in categories],
        "brand": [item[1] for item in categories],
        "list_price_centavos": [
            int(price) for price in rng.integers(1_500, 180_001, product_count)
        ],
        "is_active": [bool(flag) for flag in rng.random(product_count) < 0.94],
    }

    customer_count = settings.customers
    cities = [CITIES[int(i) % len(CITIES)] for i in rng.integers(0, len(CITIES), customer_count)]
    customers = {
        "customer_key": list(range(1, customer_count + 1)),
        "customer_id": [f"C{i:07d}" for i in range(1, customer_count + 1)],
        "given_name": [GIVEN_NAMES[i % len(GIVEN_NAMES)] for i in range(customer_count)],
        "family_name": [f"Sintetico{i:06d}" for i in range(1, customer_count + 1)],
        "city": [city[0] for city in cities],
        "state_code": [city[1] for city in cities],
        "state_name": [city[2] for city in cities],
        "loyalty_tier": [LOYALTY[i % len(LOYALTY)] for i in range(customer_count)],
        "email": [f"c{i:07d}@sintetico.invalid" for i in range(1, customer_count + 1)],
    }

    store_count = settings.stores
    store_cities = [CITIES[i % len(CITIES)] for i in range(store_count)]
    opened = [
        date(2016, 1, 1) + timedelta(days=int(offset))
        for offset in rng.integers(0, 2_500, store_count)
    ]
    names = ["Mercado Alba Digital"] + [
        f"Tienda Sintetica {i:03d}" for i in range(2, store_count + 1)
    ]
    channels = ["online"] + ["physical"] * (store_count - 1)
    stores = {
        "store_key": list(range(1, store_count + 1)),
        "store_id": [f"S{i:06d}" for i in range(1, store_count + 1)],
        "store_name": names,
        "channel": channels,
        "city": [city[0] for city in store_cities],
        "state_code": [city[1] for city in store_cities],
        "state_name": [city[2] for city in store_cities],
        "postal_code": [f"{int(code):05d}" for code in rng.integers(0, 100_000, store_count)],
        "opened_on": opened,
        "is_active": [True] + [bool(flag) for flag in rng.random(store_count - 1) < 0.94],
    }
    return Population(products=products, customers=customers, stores=stores)


def _extract_ts(day: date) -> datetime:
    return datetime(day.year, day.month, day.day, 6, 0, 0)


def _with_extract(
    columns: dict[str, list], day: date, entity: str, row_count: int
) -> dict[str, list]:
    stamped = dict(columns)
    stamped["extract_ts"] = [_extract_ts(day)] * row_count
    stamped["extract_date"] = [day] * row_count
    stamped["source_file"] = [source_file_name(entity, day)] * row_count
    return stamped


def _order_ids(day: date, count: int, rng: np.random.Generator) -> list[str]:
    ids: list[str] = []
    order_seq = 0
    remaining = 0
    for _ in range(count):
        if remaining == 0:
            order_seq += 1
            remaining = int(rng.integers(1, 4))
        remaining -= 1
        ids.append(f"O{day:%Y%m%d}{order_seq:07d}")
    return ids


def write_day(settings: Settings, population: Population, day: date) -> DayStats:
    product_count = settings.products
    customer_count = settings.customers
    store_count = settings.stores
    line_count = order_lines_for_day(
        day, settings.order_lines_per_day, settings.vary_volume_by_weekday
    )
    rng = _rng(settings.seed, day.toordinal(), 3)

    write_parquet(
        partition_path(settings.landing_root, "products", day),
        _table(RAW_PRODUCTS, _with_extract(population.products, day, "products", product_count)),
    )
    write_parquet(
        partition_path(settings.landing_root, "customers", day),
        _table(
            RAW_CUSTOMERS, _with_extract(population.customers, day, "customers", customer_count)
        ),
    )
    write_parquet(
        partition_path(settings.landing_root, "stores", day),
        _table(RAW_STORES, _with_extract(population.stores, day, "stores", store_count)),
    )

    inventory_rng = _rng(settings.seed, day.toordinal(), 5)
    inventory_rows = store_count * product_count
    store_index = np.repeat(np.arange(store_count), product_count)
    product_index = np.tile(np.arange(product_count), store_count)
    on_hand = inventory_rng.integers(0, 80, inventory_rows)
    reserved = np.minimum(inventory_rng.integers(0, 40, inventory_rows), on_hand)
    store_ids = population.stores["store_id"]
    product_ids = population.products["product_id"]
    inventory = {
        "store_id": [store_ids[int(i)] for i in store_index],
        "product_id": [product_ids[int(i)] for i in product_index],
        "snapshot_date": [day] * inventory_rows,
        "on_hand_units": [int(v) for v in on_hand],
        "reserved_units": [int(v) for v in reserved],
    }
    write_parquet(
        partition_path(settings.landing_root, "daily_inventory", day),
        _table(
            RAW_INVENTORY,
            _with_extract(inventory, day, "daily_inventory", inventory_rows),
        ),
    )

    product_choice = rng.integers(0, product_count, line_count)
    customer_choice = rng.integers(0, customer_count, line_count)
    physical_choice = rng.integers(1, store_count, line_count)
    online = rng.random(line_count) < 0.35
    store_choice = np.where(online, 0, physical_choice)
    roll = rng.random(line_count)
    quantities = rng.integers(1, 6, line_count)
    minute_of_day = rng.integers(8 * 60, 22 * 60, line_count)

    prices = population.products["list_price_centavos"]
    categories = population.products["category"]
    channels = population.stores["channel"]
    line_types: list[str] = []
    statuses: list[str] = []
    signed_qty: list[int] = []
    unit_prices: list[int] = []
    line_amounts: list[int] = []
    for index in range(line_count):
        price = int(prices[int(product_choice[index])])
        qty = int(quantities[index])
        unit_prices.append(price)
        if roll[index] < 0.92:
            line_types.append("sale")
            statuses.append("completed")
            signed_qty.append(qty)
            line_amounts.append(qty * price)
        elif roll[index] < 0.98:
            line_types.append("sale")
            statuses.append("cancelled")
            signed_qty.append(qty)
            line_amounts.append(qty * price)
        else:
            line_types.append("return")
            statuses.append("completed")
            signed_qty.append(-qty)
            line_amounts.append(-qty * price)

    local_midnight = datetime(day.year, day.month, day.day, tzinfo=MX)
    timestamps = [
        (local_midnight + timedelta(minutes=int(offset))).astimezone(UTC).replace(tzinfo=None)
        for offset in minute_of_day
    ]
    lines = {
        "order_line_id": [f"L{day:%Y%m%d}{i:07d}" for i in range(line_count)],
        "order_id": _order_ids(day, line_count, rng),
        "customer_id": [population.customers["customer_id"][int(i)] for i in customer_choice],
        "store_id": [store_ids[int(i)] for i in store_choice],
        "product_id": [product_ids[int(i)] for i in product_choice],
        "quantity": signed_qty,
        "unit_price_centavos": unit_prices,
        "line_amount_centavos": line_amounts,
        "line_type": line_types,
        "channel": [channels[int(i)] for i in store_choice],
        "status": statuses,
        "category": [categories[int(i)] for i in product_choice],
        "order_ts": timestamps,
        "order_date": [day] * line_count,
    }
    write_parquet(
        partition_path(settings.landing_root, "order_lines", day),
        _table(RAW_ORDER_LINES, _with_extract(lines, day, "order_lines", line_count)),
    )
    return DayStats(
        day=day,
        order_lines=line_count,
        products=product_count,
        customers=customer_count,
        stores=store_count,
        inventory_rows=inventory_rows,
    )


def generate_days(settings: Settings, start: date, end: date) -> list[DayStats]:
    population = build_population(settings)
    return [write_day(settings, population, day) for day in iter_dates(start, end)]


def raw_column_order() -> dict[str, list[str]]:
    return {
        entity: column_names(columns)
        for entity, columns in {
            "order_lines": RAW_ORDER_LINES,
            "products": RAW_PRODUCTS,
            "customers": RAW_CUSTOMERS,
            "stores": RAW_STORES,
            "daily_inventory": RAW_INVENTORY,
        }.items()
    }
