"""Date dimension, including the annual Mexican public rest days.

Observed Mondays follow the recurring pattern in the Ley Federal del Trabajo
(first Monday of February, third Monday of March, third Monday of November)
plus the fixed dates. One-off civic dates, such as a presidential inauguration,
are left out. This is a calendar attribute for a fictitious company.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date, timedelta

DAY_NAMES = (
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
)

CALENDAR_START = date(2024, 1, 1)
CALENDAR_END = date(2028, 12, 31)


def nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """Return the nth ``weekday`` (Monday = 0) in a month."""

    first = date(year, month, 1)
    shift = (weekday - first.weekday()) % 7
    return first + timedelta(days=shift + 7 * (n - 1))


def holiday_name(day: date) -> str | None:
    named = {
        date(day.year, 1, 1): "Año Nuevo",
        nth_weekday(day.year, 2, 0, 1): "Día de la Constitución",
        nth_weekday(day.year, 3, 0, 3): "Natalicio de Benito Juárez",
        date(day.year, 5, 1): "Día del Trabajo",
        date(day.year, 9, 16): "Día de la Independencia",
        nth_weekday(day.year, 11, 0, 3): "Día de la Revolución",
        date(day.year, 12, 25): "Navidad",
    }
    return named.get(day)


def iter_dates(start: date, end: date) -> Iterator[date]:
    if end < start:
        raise ValueError(f"end {end} is before start {start}")
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def dim_date_rows(
    start: date = CALENDAR_START,
    end: date = CALENDAR_END,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for day in iter_dates(start, end):
        holiday = holiday_name(day)
        rows.append(
            {
                "date_key": day.year * 10_000 + day.month * 100 + day.day,
                "full_date": day,
                "year": day.year,
                "quarter": (day.month - 1) // 3 + 1,
                "month": day.month,
                "day": day.day,
                "day_name": DAY_NAMES[day.weekday()],
                "is_weekend": day.weekday() >= 5,
                "is_mx_public_holiday": holiday is not None,
                "holiday_name": holiday or "",
            }
        )
    return rows
