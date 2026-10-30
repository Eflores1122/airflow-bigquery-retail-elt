"""GCS-style landing layout on the local filesystem.

Objects live at ``<root>/<entity>/dt=YYYY-MM-DD/part-000.parquet``. A BigQuery
deployment uses the same relative path under a ``gs://`` prefix. The local
adapter reads the files directly, so no cloud credentials are required.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

from mercadoalba.calendar_mx import iter_dates

LOGGER = logging.getLogger(__name__)

ENTITIES = ("order_lines", "products", "customers", "stores", "daily_inventory")


class LandingNotFoundError(FileNotFoundError):
    """A required landing partition is not on disk."""


def partition_path(root: Path, entity: str, day: date) -> Path:
    if entity not in ENTITIES:
        raise ValueError(f"unknown landing entity {entity}")
    return root / entity / f"dt={day.isoformat()}" / "part-000.parquet"


def source_file_name(entity: str, day: date) -> str:
    return f"{entity}/dt={day.isoformat()}/part-000.parquet"


def list_partition_files(root: Path, entity: str, days: list[date] | None) -> list[Path]:
    """Return parquet parts for ``entity``.

    ``days is None`` lists every partition. A concrete list returns only those
    dates and skips dates that have no file.
    """

    if entity not in ENTITIES:
        raise ValueError(f"unknown landing entity {entity}")
    if days is None:
        return sorted((root / entity).glob("dt=*/part-*.parquet"))
    found: list[Path] = []
    for day in days:
        found.extend(sorted(partition_path(root, entity, day).parent.glob("part-*.parquet")))
    return found


def partition_dates(root: Path, entity: str) -> list[date]:
    dates: list[date] = []
    for path in list_partition_files(root, entity, None):
        label = path.parent.name
        if label.startswith("dt="):
            dates.append(date.fromisoformat(label.removeprefix("dt=")))
    return sorted(set(dates))


@dataclass(frozen=True)
class RunWindow:
    """Inclusive date window for one ELT run. Serialized through Airflow XCom."""

    mode: str
    logical_date: date
    window_start: date
    window_end: date
    generate: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "logical_date": self.logical_date.isoformat(),
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "generate": self.generate,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> RunWindow:
        return cls(
            mode=str(payload["mode"]),
            logical_date=date.fromisoformat(str(payload["logical_date"])),
            window_start=date.fromisoformat(str(payload["window_start"])),
            window_end=date.fromisoformat(str(payload["window_end"])),
            generate=bool(payload["generate"]),
        )


def make_window(
    *,
    mode: str,
    logical_date: date,
    lookback_days: int,
    history_start: date | None,
    generate: bool,
) -> RunWindow:
    if mode not in {"full_refresh", "incremental"}:
        raise ValueError("mode must be 'full_refresh' or 'incremental'")
    if lookback_days < 0:
        raise ValueError("lookback_days must be >= 0")
    if mode == "incremental":
        start = logical_date - timedelta(days=lookback_days)
    else:
        start = history_start or logical_date
    if start > logical_date:
        raise ValueError("window start is after the logical date")
    return RunWindow(
        mode=mode,
        logical_date=logical_date,
        window_start=start,
        window_end=logical_date,
        generate=generate,
    )


def coerce_window(window: RunWindow | dict[str, object]) -> RunWindow:
    if isinstance(window, RunWindow):
        return window
    return RunWindow.from_dict(window)


@dataclass(frozen=True)
class LandingManifest:
    """Files a run will read, and the dates an incremental merge may touch."""

    order_line_files: tuple[Path, ...]
    product_files: tuple[Path, ...]
    customer_files: tuple[Path, ...]
    store_files: tuple[Path, ...]
    inventory_files: tuple[Path, ...]
    # None means the full refresh reloads every partition it finds.
    incremental_dates: tuple[date, ...] | None

    @property
    def order_line_files_read(self) -> int:
        return len(self.order_line_files)


def build_manifest(root: Path, window: RunWindow) -> LandingManifest:
    """Resolve landing files for a run.

    Incremental runs require the logical date and quietly skip missing lookback
    dates so the first day of a dataset still loads. Full refresh reads every
    partition already on disk and still requires the logical date.
    """

    logical_lines = list_partition_files(root, "order_lines", [window.logical_date])
    if not logical_lines:
        raise LandingNotFoundError(
            f"no order-line partition for logical date {window.logical_date.isoformat()} "
            f"under {root}"
        )

    if window.mode == "full_refresh":
        return LandingManifest(
            order_line_files=tuple(list_partition_files(root, "order_lines", None)),
            product_files=tuple(list_partition_files(root, "products", None)),
            customer_files=tuple(list_partition_files(root, "customers", None)),
            store_files=tuple(list_partition_files(root, "stores", None)),
            inventory_files=tuple(list_partition_files(root, "daily_inventory", None)),
            incremental_dates=None,
        )

    present: list[date] = []
    for day in iter_dates(window.window_start, window.window_end):
        if list_partition_files(root, "order_lines", [day]):
            present.append(day)
        else:
            LOGGER.warning("skipping missing lookback partition %s", day.isoformat())

    for day in present:
        for entity in ("products", "customers", "stores", "daily_inventory"):
            if not list_partition_files(root, entity, [day]):
                raise LandingNotFoundError(
                    f"order-line partition {day.isoformat()} is missing {entity}"
                )

    return LandingManifest(
        order_line_files=tuple(list_partition_files(root, "order_lines", present)),
        product_files=tuple(list_partition_files(root, "products", present)),
        customer_files=tuple(list_partition_files(root, "customers", present)),
        store_files=tuple(list_partition_files(root, "stores", present)),
        inventory_files=tuple(list_partition_files(root, "daily_inventory", present)),
        incremental_dates=tuple(present),
    )
