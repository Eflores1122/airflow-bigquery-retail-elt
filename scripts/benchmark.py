"""Measure local DuckDB runtimes for a full refresh and a partition merge.

Generation is not included. The JSON records local synthetic runs only.

The README section between the BENCHMARK markers is replaced with the table
from this run unless ``--no-readme`` is passed.
"""

from __future__ import annotations

import argparse
import json
import platform
import shutil
import sys
import tempfile
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import duckdb  # noqa: E402

from mercadoalba.calendar_mx import iter_dates  # noqa: E402
from mercadoalba.config import Settings  # noqa: E402
from mercadoalba.pipeline import RunReport, run_pipeline  # noqa: E402


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[mid]
    return (ordered[mid - 1] + ordered[mid]) / 2


def _copy_db(source: Path, dest: Path) -> None:
    if dest.exists():
        dest.unlink()
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, dest)
    wal = Path(str(source) + ".wal")
    if wal.exists():
        shutil.copy2(wal, Path(str(dest) + ".wal"))


def _fresh(path: Path) -> None:
    if path.exists():
        path.unlink()
    wal = Path(str(path) + ".wal")
    if wal.exists():
        wal.unlink()


def _summarize(samples: list[RunReport]) -> dict[str, object]:
    def column(name: str) -> list[float]:
        return [sample.timings_seconds[name] for sample in samples]

    return {
        "seconds_median": round(_median(column("total")), 3),
        "seconds_min": round(min(column("total")), 3),
        "seconds_max": round(max(column("total")), 3),
        "load_and_stage_seconds_median": round(_median(column("load_and_stage")), 3),
        "publish_seconds_median": round(_median(column("publish")), 3),
        "quality_seconds_median": round(_median(column("quality")), 3),
        "order_line_files_read": samples[-1].order_line_files_read,
        "fact_order_lines": samples[-1].fact_order_lines,
        "staging_order_lines": samples[-1].staging_order_lines,
        "samples_total_seconds": [round(value, 3) for value in column("total")],
    }


def _markdown(results: dict) -> str:
    dataset = results["dataset"]
    machine = results["machine"]
    scenarios = results["scenarios"]

    def row(label: str, name: str) -> str:
        item = scenarios[name]
        return (
            f"| {label} | {item['seconds_median']:.3f} | "
            f"{item['load_and_stage_seconds_median']:.3f} | "
            f"{item['publish_seconds_median']:.3f} | "
            f"{item['quality_seconds_median']:.3f} | "
            f"{item['order_line_files_read']} |"
        )

    days = dataset["days"]
    ratio = results["ratio_full_refresh_to_incremental_lookback"]
    transform_ratio = results["ratio_transform_full_refresh_to_incremental_lookback"]
    intro = (
        f"Measured on {machine['platform']}, Python {machine['python']}, "
        f"DuckDB {machine['duckdb']}, {machine['cpu_count']} CPUs. "
        f"Median of {dataset['iterations']} runs after {dataset['warmup']} warmup. "
        "Generation is excluded. Parquet stays in the OS page cache after warmup, "
        "which favors the full refresh. Each full-refresh sample starts from an empty "
        "DuckDB file. Each incremental sample starts from a copy of a warehouse that "
        "already holds every day."
    )
    header = (
        "| Scenario | Median total (s) | Median load + stage (s) | "
        "Median publish (s) | Median quality (s) | Order-line files |"
    )
    dataset_line = (
        f"Dataset: {dataset['fact_order_lines']:,} fact rows across {days} daily "
        f"partitions, {dataset['products']:,} products, {dataset['stores']:,} stores, "
        f"{dataset['customers']:,} customers, seed {dataset['seed']}. Weekday volume "
        "makes some days larger than `--order-lines-per-day`."
    )
    day_ratio = results["ratio_full_refresh_to_incremental_day"]
    ratios = (
        "Steady-state ratio of median total time, full refresh / one-partition "
        f"incremental: **{day_ratio:.2f}x**. Full refresh / scheduled lookback-1 "
        f"incremental: **{ratio:.2f}x**. The same ratios for load+stage+publish, "
        "excluding quality gates, are "
        f"**{results['ratio_transform_full_refresh_to_incremental_day']:.2f}x** "
        f"and **{transform_ratio:.2f}x**. The backfill row is one pass that applies "
        "every partition from an empty warehouse. A cold historical load can be "
        "cheaper as a single full refresh. The daily win is the steady-state merge, "
        "whose cost stays tied to the window rather than to retained history."
    )
    closing = (
        "These figures are local synthetic runs only. They describe this DuckDB "
        "dataset on this machine."
    )
    table = "\n".join(
        [
            header,
            "| --- | ---: | ---: | ---: | ---: | ---: |",
            row(f"Full refresh of {days} days", "full_refresh_all_history"),
            row("Incremental, lookback 0 (one partition)", "incremental_lookback_zero"),
            row("Incremental, lookback 1 (scheduled DAG)", "incremental_lookback_one_day"),
            row(
                f"Incremental backfill of {days} days (one sample)",
                "incremental_backfill_from_empty",
            ),
        ]
    )
    return "\n\n".join([intro, table, dataset_line, ratios, closing])


def _splice_readme(markdown: str) -> None:
    readme = ROOT / "README.md"
    text = readme.read_text(encoding="utf-8")
    start = "<!-- BENCHMARK:START -->"
    end = "<!-- BENCHMARK:END -->"
    if start not in text or end not in text:
        raise SystemExit("README is missing BENCHMARK markers")
    pre, rest = text.split(start, 1)
    _, post = rest.split(end, 1)
    readme.write_text(f"{pre}{start}\n\n{markdown}\n\n{end}{post}", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=21)
    parser.add_argument("--order-lines-per-day", type=int, default=60_000)
    parser.add_argument("--products", type=int, default=1_000)
    parser.add_argument("--stores", type=int, default=20)
    parser.add_argument("--customers", type=int, default=8_000)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=1)
    parser.add_argument("--output", type=Path, default=ROOT / "benchmarks" / "results.json")
    parser.add_argument("--no-readme", action="store_true")
    args = parser.parse_args(argv)
    if args.days < 2:
        raise SystemExit("--days must be at least 2 so lookback 1 has two partitions")

    start = date_start()
    end = start + timedelta(days=args.days - 1)
    with tempfile.TemporaryDirectory(prefix="mercadoalba-bench-") as temporary:
        root = Path(temporary)
        base = Settings(
            duckdb_path=root / "unused.duckdb",
            landing_root=root / "landing",
            products=args.products,
            stores=args.stores,
            customers=args.customers,
            order_lines_per_day=args.order_lines_per_day,
            lookback_days=0,
            transform_attempts=1,
            vary_volume_by_weekday=True,
            seed=42,
        )
        seed_path = root / "seed.duckdb"
        seeded = replace(base, duckdb_path=seed_path)
        print(f"generating and loading {args.days} days into a seed warehouse", flush=True)
        seed_report = run_pipeline(
            seeded,
            mode="full_refresh",
            logical_date=end,
            history_start=start,
            lookback_days=0,
            generate=True,
        )

        def run_sample(
            mode: str, logical, lookback: int, path: Path, *, copy_from: Path | None, fresh: bool
        ) -> RunReport:
            if copy_from is not None:
                _copy_db(copy_from, path)
            elif fresh:
                _fresh(path)
            tuned = replace(base, duckdb_path=path)
            return run_pipeline(
                tuned,
                mode=mode,
                logical_date=logical,
                lookback_days=lookback,
                generate=False,
            )

        def collect(label: str, fn, iterations: int, warmup: int) -> list[RunReport]:
            print(f"{label}: warmup x{warmup}", flush=True)
            for _ in range(warmup):
                fn()
            samples: list[RunReport] = []
            for index in range(iterations):
                print(f"{label}: sample {index + 1}/{iterations}", flush=True)
                samples.append(fn())
            return samples

        full_samples = collect(
            "full refresh",
            lambda: run_sample(
                "full_refresh",
                end,
                0,
                root / "full.duckdb",
                copy_from=None,
                fresh=True,
            ),
            args.iterations,
            args.warmup,
        )
        lookback_zero = collect(
            "incremental lookback 0",
            lambda: run_sample(
                "incremental",
                end,
                0,
                root / "inc0.duckdb",
                copy_from=seed_path,
                fresh=False,
            ),
            args.iterations,
            args.warmup,
        )
        lookback_one = collect(
            "incremental lookback 1",
            lambda: run_sample(
                "incremental",
                end,
                1,
                root / "inc1.duckdb",
                copy_from=seed_path,
                fresh=False,
            ),
            args.iterations,
            args.warmup,
        )
        print("incremental backfill from empty", flush=True)
        backfill_db = root / "backfill.duckdb"
        _fresh(backfill_db)
        backfill_samples = [
            run_sample(
                "incremental",
                day,
                0,
                backfill_db,
                copy_from=None,
                fresh=False,
            )
            for day in iter_dates(start, end)
        ]

    if full_samples[-1].order_line_files_read != args.days:
        raise SystemExit("full refresh did not read every order-line partition")
    if lookback_zero[-1].order_line_files_read != 1:
        raise SystemExit("lookback 0 did not read exactly one order-line partition")
    if lookback_one[-1].order_line_files_read != 2:
        raise SystemExit("lookback 1 did not read exactly two order-line partitions")

    full = _summarize(full_samples)
    zero = _summarize(lookback_zero)
    one = _summarize(lookback_one)
    backfill_totals = [sample.timings_seconds["total"] for sample in backfill_samples]
    backfill = {
        "seconds_median": round(sum(backfill_totals), 3),
        "seconds_min": round(sum(backfill_totals), 3),
        "seconds_max": round(sum(backfill_totals), 3),
        "load_and_stage_seconds_median": round(
            sum(sample.timings_seconds["load_and_stage"] for sample in backfill_samples),
            3,
        ),
        "publish_seconds_median": round(
            sum(sample.timings_seconds["publish"] for sample in backfill_samples),
            3,
        ),
        "quality_seconds_median": round(
            sum(sample.timings_seconds["quality"] for sample in backfill_samples),
            3,
        ),
        "order_line_files_read": args.days,
        "fact_order_lines": backfill_samples[-1].fact_order_lines,
        "staging_order_lines": sum(sample.staging_order_lines for sample in backfill_samples),
        "samples_total_seconds": [round(sum(backfill_totals), 3)],
        "note": (
            "Single pass. seconds_median is the sum of per-day totals, not a statistical median."
        ),
    }
    transform_full = full["load_and_stage_seconds_median"] + full["publish_seconds_median"]
    transform_one = one["load_and_stage_seconds_median"] + one["publish_seconds_median"]
    transform_zero = zero["load_and_stage_seconds_median"] + zero["publish_seconds_median"]
    if min(one["seconds_median"], zero["seconds_median"], transform_one, transform_zero) <= 0:
        raise SystemExit(
            "incremental timing rounded to zero; increase --days or --order-lines-per-day"
        )
    results = {
        "measured_at_utc": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "disclaimer": (
            "Local DuckDB timings on synthetic retail data only. "
            "These figures are local synthetic runs."
        ),
        "machine": {
            "platform": platform.platform(),
            "python": platform.python_version(),
            "duckdb": duckdb.__version__,
            "cpu_count": __import__("os").cpu_count(),
        },
        "dataset": {
            "days": args.days,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "order_lines_per_day_base": args.order_lines_per_day,
            "fact_order_lines": seed_report.fact_order_lines,
            "products": args.products,
            "stores": args.stores,
            "customers": args.customers,
            "seed": 42,
            "iterations": args.iterations,
            "warmup": args.warmup,
            "vary_volume_by_weekday": True,
        },
        "scenarios": {
            "full_refresh_all_history": full,
            "incremental_lookback_zero": zero,
            "incremental_lookback_one_day": one,
            "incremental_backfill_from_empty": backfill,
        },
        "ratio_full_refresh_to_incremental_day": round(
            full["seconds_median"] / zero["seconds_median"],
            2,
        ),
        "ratio_full_refresh_to_incremental_lookback": round(
            full["seconds_median"] / one["seconds_median"],
            2,
        ),
        "ratio_transform_full_refresh_to_incremental_day": round(
            transform_full / transform_zero,
            2,
        ),
        "ratio_transform_full_refresh_to_incremental_lookback": round(
            transform_full / transform_one,
            2,
        ),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
    markdown = _markdown(results)
    print(markdown)
    if not args.no_readme:
        _splice_readme(markdown)
        print(f"updated {ROOT / 'README.md'}")
    print(f"wrote {args.output}")
    return 0


def date_start():
    from datetime import date

    return date(2026, 1, 5)


if __name__ == "__main__":
    raise SystemExit(main())
