"""MonitoringScheduler: the periodic "check every open position now" driver.

This is distinct from ``scheduler/platform_scheduler.py`` (which fires
wall-clock time triggers like entry at 09:20 and the hard cutoff at 15:15).
This one runs a steady heartbeat *between* those triggers and, on each beat,
asks every registered runner to re-evaluate its open position via
``StrategyRunner.dispatch_monitor_cycle`` -> ``Strategy.on_monitor_cycle``.

Why it exists (resolves review finding C1): a strategy's stop-loss and target
must be checked continuously through the day, not only at the cutoff. The push
path (live websocket ticks -> ``dispatch_tick``) is the low-latency route, but
it only works when a live tick feed is connected and flowing. This scheduler is
the *guaranteed* route: it drives the monitor's poll-and-check on a fixed
cadence, and the monitor falls back to polling the broker for fresh prices when
the tick feed is stale or absent. With both wired, an open straddle is always
evaluated -- promptly on ticks when the feed is healthy, and at worst every
``interval_seconds`` regardless.

Concurrency model, matching ``PlatformScheduler``: one background daemon thread
wakes on a fixed interval and dispatches to every RUNNING runner in turn. It is
strategy-agnostic (it only knows "ask this runner to run a monitor cycle"), and
each dispatch goes through the runner's own per-instance lock and fault
isolation, so one instance's failure never stops the others or the loop.
"""

from __future__ import annotations

import logging
import threading
import time as _time_module
from collections.abc import Callable
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field

from algo.strategy_engine.strategy_runner import RunnerStatus

if TYPE_CHECKING:
    from algo.strategy_engine.strategy_runner import StrategyRunner


class MonitoringSchedulerConfig(BaseModel):
    """Tuning for the monitoring heartbeat."""

    model_config = ConfigDict(frozen=True)

    interval_seconds: float = Field(
        default=2.0,
        gt=0,
        description="How often to ask every registered runner to re-check its "
        "open position. Bounds the worst-case delay between a stop-loss/target "
        "being crossed and it being acted on, when the live tick feed is not "
        "delivering. Must not be set so low that it breaches the broker's "
        "MARKET_DATA rate limit across all instruments.",
    )


class MonitoringScheduler:
    """Drives ``StrategyRunner.dispatch_monitor_cycle`` on a fixed cadence.

    All collaborators are injected (the sleep function and the logger) so the
    loop is testable without real time; ``_tick()`` can also be called directly
    for deterministic, thread-free assertions.
    """

    def __init__(
        self,
        *,
        config: MonitoringSchedulerConfig | None = None,
        logger: logging.Logger | None = None,
        sleep: Callable[[float], None] = _time_module.sleep,  # noqa: ARG002 -- kept for symmetry/future use
    ) -> None:
        self._config = config or MonitoringSchedulerConfig()
        self._logger = logger if logger is not None else logging.getLogger("algo.monitoring_scheduler")
        self._lock = threading.Lock()
        self._runners: dict[str, StrategyRunner] = {}
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    # -- Registration --------------------------------------------------

    def register(self, runner: StrategyRunner) -> None:
        """Bring a runner under monitoring. Safe before or after ``start()``.
        Idempotent per identity: re-registering the same identity replaces the
        entry rather than duplicating dispatch."""
        key = runner.identity_str
        with self._lock:
            self._runners[key] = runner
        self._logger.info("monitoring registered %s", key)

    def unregister(self, runner: StrategyRunner) -> None:
        """Remove a runner from monitoring. A no-op if not registered."""
        key = runner.identity_str
        with self._lock:
            self._runners.pop(key, None)

    def registered_identities(self) -> list[str]:
        with self._lock:
            return list(self._runners.keys())

    # -- Background loop -----------------------------------------------

    def start(self) -> None:
        """Start the background heartbeat thread. Idempotent: a no-op if already
        running."""
        with self._lock:
            if self._thread is not None:
                return
            self._stop_event.clear()
            thread = threading.Thread(
                target=self._run_loop, name="MonitoringScheduler", daemon=True
            )
            self._thread = thread
        thread.start()
        self._logger.info(
            "monitoring scheduler started (interval=%.2fs)", self._config.interval_seconds
        )

    def stop(self) -> None:
        """Stop the heartbeat thread and clear registrations, leaving this
        instance reusable for a later start/register cycle. Idempotent. Does
        not stop the runners themselves -- that is the platform scheduler's and
        container's job."""
        with self._lock:
            thread = self._thread
            self._thread = None
            self._stop_event.set()
        if thread is not None:
            thread.join(timeout=self._config.interval_seconds + 5.0)
            if thread.is_alive():
                self._logger.critical(
                    "monitoring scheduler thread did not stop within the timeout"
                )
        with self._lock:
            self._runners.clear()
        self._logger.info("monitoring scheduler stopped")

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception:  # noqa: BLE001 -- the loop must survive an unexpected failure
                self._logger.critical("monitoring tick raised unexpectedly", exc_info=True)
            self._stop_event.wait(self._config.interval_seconds)

    def _tick(self) -> None:
        """One heartbeat: ask every RUNNING runner to re-check its position.
        Synchronous and side-effect-scoped so tests can call it directly."""
        with self._lock:
            runners = list(self._runners.values())
        for runner in runners:
            if runner.status is not RunnerStatus.RUNNING:
                continue
            # dispatch_monitor_cycle already contains its own fault isolation
            # (a hook failure freezes only that instance); this guard is a
            # belt-and-suspenders against anything escaping that contract, so
            # one runner can never stop the heartbeat for the others.
            try:
                runner.dispatch_monitor_cycle()
            except Exception:  # noqa: BLE001
                self._logger.critical(
                    "dispatch_monitor_cycle raised for %s (should have been contained)",
                    runner.identity_str,
                    exc_info=True,
                )
