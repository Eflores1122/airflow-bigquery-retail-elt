"""Small retry helper for transient warehouse and filesystem errors.

Airflow retries a failed task as well. This helper lets the CLI and the tests
exercise the same idea without a scheduler. Data-quality failures are raised
outside the helper so a deterministic gate is not retried blindly.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

LOGGER = logging.getLogger(__name__)


def call_with_retries[T](
    fn: Callable[[], T],
    *,
    attempts: int,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
) -> T:
    """Call ``fn`` until it returns or ``attempts`` are exhausted."""

    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    last: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except retry_on as exc:
            last = exc
            if attempt == attempts:
                raise
            LOGGER.warning("attempt %s/%s failed: %s", attempt, attempts, exc)
    raise RuntimeError("retry loop exited without a result") from last
