"""Clock abstraction so SimulationBroker's timing is swappable: real
wall-clock sleep for paper-trading realism, or an instant, fully deterministic
clock for fast unit tests -- without SimulationBroker itself branching on
which mode it's in.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Protocol


class Clock(Protocol):
    """Everything SimulationBroker needs from time: the current instant, and
    a way to wait -- kept this narrow so a test double never has to
    implement more than these two methods."""

    def now(self) -> datetime: ...

    def sleep(self, seconds: float) -> None: ...


class SystemClock:
    """Real wall-clock time and real sleeping. The default for paper trading:
    fills genuinely take the configured latency to resolve, matching how a
    real broker behaves."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)

    def sleep(self, seconds: float) -> None:
        if seconds > 0:
            time.sleep(seconds)


class ManualClock:
    """Fully deterministic clock for tests: now() never advances on its own
    and sleep() never blocks -- a test controls elapsed time explicitly via
    advance(), so assertions on timestamps are exact and repeatable."""

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start if start is not None else datetime(2026, 1, 1, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self._now

    def sleep(self, seconds: float) -> None:
        return None

    def advance(self, seconds: float) -> None:
        self._now += timedelta(seconds=seconds)
