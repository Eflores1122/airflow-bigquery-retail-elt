"""Command-line entry for a local DuckDB run.

Examples
--------
python -m mercadoalba --mode full-refresh --date 2026-03-02 --history-start 2026-03-01
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import date
from pathlib import Path

from mercadoalba.config import Settings
from mercadoalba.landing import LandingNotFoundError
from mercadoalba.pipeline import run_pipeline
from mercadoalba.quality import DataQualityError


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the fictitious Mercado Alba ELT on local DuckDB. Synthetic data only."
    )
    parser.add_argument("--mode", required=True, choices=["full-refresh", "incremental"])
    parser.add_argument("--date", required=True, help="Logical business date, YYYY-MM-DD.")
    parser.add_argument(
        "--history-start",
        help="First date to generate for a full refresh. Ignored by incremental mode.",
    )
    parser.add_argument("--lookback-days", type=int, default=1)
    parser.add_argument("--landing-root", type=Path, default=Path("data/landing"))
    parser.add_argument(
        "--duckdb-path",
        type=Path,
        default=Path("data/warehouse/mercadoalba.duckdb"),
    )
    parser.add_argument("--order-lines-per-day", type=int, default=5_000)
    parser.add_argument("--customers", type=int, default=4_000)
    parser.add_argument("--products", type=int, default=400)
    parser.add_argument("--stores", type=int, default=12)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument(
        "--no-weekday-volume",
        action="store_true",
        help="Keep every day at --order-lines-per-day instead of a weekday curve.",
    )
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Read partitions already in the landing root.",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    settings = Settings(
        duckdb_path=args.duckdb_path,
        landing_root=args.landing_root,
        seed=args.seed,
        products=args.products,
        stores=args.stores,
        customers=args.customers,
        order_lines_per_day=args.order_lines_per_day,
        lookback_days=args.lookback_days,
        transform_attempts=args.attempts,
        vary_volume_by_weekday=not args.no_weekday_volume,
    )
    mode = "full_refresh" if args.mode == "full-refresh" else "incremental"
    history = date.fromisoformat(args.history_start) if args.history_start else None
    try:
        report = run_pipeline(
            settings,
            mode=mode,
            logical_date=date.fromisoformat(args.date),
            lookback_days=args.lookback_days,
            history_start=history,
            generate=not args.skip_generate,
        )
    except DataQualityError as exc:
        print(json.dumps(exc.report.as_dict(), indent=2))
        return 1
    except LandingNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(report.as_dict(), indent=2))
    return 0


def cli() -> None:
    raise SystemExit(main())


if __name__ == "__main__":
    cli()
