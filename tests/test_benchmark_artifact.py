"""The README quotes the committed local benchmark."""

from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_readme_quotes_measured_benchmark() -> None:
    artifact = ROOT / "benchmarks" / "results.json"
    assert artifact.exists(), "run python scripts/benchmark.py to record local timings"
    results = json.loads(artifact.read_text(encoding="utf-8"))
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "local synthetic" in readme.lower()
    assert "local synthetic" in results["disclaimer"].lower()
    for name in (
        "full_refresh_all_history",
        "incremental_lookback_zero",
        "incremental_lookback_one_day",
    ):
        median = results["scenarios"][name]["seconds_median"]
        assert f"{median:.3f}" in readme
    assert results["scenarios"]["incremental_lookback_zero"]["order_line_files_read"] == 1
    assert (
        results["scenarios"]["full_refresh_all_history"]["order_line_files_read"]
        == results["dataset"]["days"]
    )
