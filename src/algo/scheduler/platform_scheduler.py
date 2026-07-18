"""PlatformScheduler: fires each registered strategy instance's declared
time-of-day triggers (entry, hard cutoff, ...) at the right wall-clock time.

This is the top-level daily job scheduler ("what time is it, what job fires
now"), distinct from ``strategy_engine/strategy_scheduler.py`` (per-instance
tick-monitoring cadence -- a separate, not-yet-built concern; see the
assumptions in this task's summary for the boundary).

Concurrency model, resolved here (closing the TODO the original stub left
open): thread-based, matching the rest of the platform (sync SQLAlchemy, the
market-data websocket on its own thread, StrategyRunner's per-instance lock).
One background thread wakes on a configurable interval, checks every
registered instance's due triggers, and dispatches them -- there is no
per-trigger timer and no asyncio anywhere in this module.

Strategy-agnostic and trades nothing: this module knows a trigger's *name* and
*time*, never what the trigger *means*. "Schedule strategy entry" and "schedule
strategy exit" are true only in the sense that Strategy-1 happens to declare
triggers named "entry" and "cutoff" (Task 17) -- the scheduler would fire a
trigger named anything, for any strategy, identically. All actual trading
decisions live inside ``Strategy.on_time_trigger``, several layers away from
this file.

Integration points:

* **StrategyRunner** -- ``register()`` calls ``runner.start()`` (which is the
  restart-recovery trigger: it runs ``Strategy.recover()`` before this
  scheduler ever dispatches to it), reads ``runner.scheduled_triggers()``
  once, and later calls ``runner.dispatch_time_trigger(name)`` when each is
  due. ``stop()`` calls ``runner.stop()`` for every registered runner,
  completing the lifecycle this scheduler began.
* **The state machine** -- indirectly, via ``RunnerStatus`` (StrategyRunner's
  own coarse lifecycle state machine): only a RUNNING runner ever receives a
  dispatch. A runner that has moved to FROZEN (an unrecoverable failure inside
  some earlier dispatch, per StrategyRunner's own fault-isolation) is silently
  skipped on every subsequent tick -- never retried, never re-armed by this
  scheduler; that is an operator's call, made elsewhere.

Missed-trigger handling: on ``register()`` (i.e. at startup/restart, or when a
new instance is added mid-run), any trigger whose time-of-day has already
passed today is resolved immediately, per its own declared policy --
``TriggerCatchUpPolicy.SKIP`` records it as already handled for today (so the
main loop never fires it, but it fires normally tomorrow); ``FIRE_ON_STARTUP``
leaves it eligible, so the very next tick fires it right away. No trigger is
ever fired more than once for the same calendar day.
"""

from __future__ import annotations

import logging
import threading
import time as _time_module
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, time

from pydantic import BaseModel, ConfigDict, Field

from algo.scheduler.trading_calendar import TradingCalendar, WeekdayTradingCalendar
from algo.strategy_engine.strategy_base import TimeTrigger, TriggerCatchUpPolicy
from algo.strategy_engine.strategy_context import TimeProvider
from algo.strategy_engine.strategy_runner import RunnerStatus, StrategyRunner


class SchedulerConfig(BaseModel):
    """Tuning for the scheduler's background loop."""

    model_config = ConfigDict(frozen=True)

    poll_interval_seconds: float = Field(
        default=1.0, gt=0,
        description="How often the background thread wakes to check for due "
        "triggers. Bounds the worst-case delay between a trigger's configured "
        "time and it actually firing.",
    )


@dataclass
class _RunnerSchedule:
    """Internal bookkeeping for one registered runner: its declared triggers
    (snapshotted once at registration) and, per trigger name, the last date it
    fired or was deliberately skipped -- comparing against "today" is what
    makes day-rollover automatic, with no explicit midnight-reset logic."""

    runner: StrategyRunner
    triggers: tuple[TimeTrigger, ...]
    fired_on: dict[str, date] = field(default_factory=dict)
    skipped_on: dict[str, date] = field(default_factory=dict)


class PlatformScheduler:
    """Fires registered ``StrategyRunner`` instances' declared time triggers.

    All collaborators are injected: the clock (``TimeProvider``), the trading-
    day check (``TradingCalendar``), the sleep function, and the logger --
    keeping the whole scheduler testable without real threads or real time
    (``_tick()`` can be called directly and deterministically; the background
    thread itself is exercised separately, with short intervals).
    """

    def __init__(
        self,
        *,
        time_provider: TimeProvider,
        config: SchedulerConfig | None = None,
        trading_calendar: TradingCalendar | None = None,
        logger: logging.Logger | None = None,
        sleep: Callable[[float], None] = _time_module.sleep,
    ) -> None:
        self._time = time_provider
        self._config = config or SchedulerConfig()
        self._calendar = trading_calendar or WeekdayTradingCalendar()
        self._logger = logger if logger is not None else logging.getLogger("algo.scheduler")
        self._sleep = sleep

        self._lock = threading.Lock()
        self._schedules: dict[str, _RunnerSchedule] = {}
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    # -- Registration (restart recovery happens here) ------------------

    def register(self, runner: StrategyRunner) -> None:
        """Bring a runner under this scheduler's management.

        Starts the runner (``StrategyRunner.start()``, which is what triggers
        the strategy's own ``recover()`` -- so restart recovery for this
        instance happens synchronously, inside this call, before the scheduler
        ever considers dispatching to it), snapshots its declared triggers, and
        resolves any already-passed-today trigger per its catch-up policy (see
        the module docstring). Safe to call before or after ``start()``; a
        schedule registered while the loop is already running is picked up on
        the next tick. Raises ``ValueError`` if this runner's identity is
        already registered.
        """
        key = runner.identity_str
        with self._lock:
            if key in self._schedules:
                raise ValueError(f"runner {key!r} is already registered")
            runner.start()
            triggers = tuple(runner.scheduled_triggers())
            schedule = _RunnerSchedule(runner=runner, triggers=triggers)
            self._seed_missed_triggers(schedule)
            self._schedules[key] = schedule
        self._logger.info(
            "registered %s with %d trigger(s): %s",
            key, len(triggers), [t.name for t in triggers],
        )

    def unregister(self, runner: StrategyRunner, *, stop_runner: bool = True) -> None:
        """Remove a runner from scheduling. By default also stops it
        (``StrategyRunner.stop()``); pass ``stop_runner=False`` if the caller
        will stop it itself. A no-op if the runner was not registered."""
        key = runner.identity_str
        with self._lock:
            schedule = self._schedules.pop(key, None)
        if schedule is None:
            return
        if stop_runner:
            schedule.runner.stop()
        self._logger.info("unregistered %s", key)

    def registered_identities(self) -> list[str]:
        """The identities of every currently-registered runner, for
        introspection/monitoring."""
        with self._lock:
            return list(self._schedules.keys())

    # -- Missed-trigger resolution (called under self._lock) -----------

    def _seed_missed_triggers(self, schedule: _RunnerSchedule) -> None:
        today = self._time.today()
        if not self._calendar.is_trading_day(today):
            # Nothing to seed: on a non-trading day the main loop's own
            # trading-day check suppresses every trigger anyway, and there is
            # nothing to "catch up" from on a day nothing was ever due.
            return
        now_time = self._time.now_ist().time()
        for trigger in schedule.triggers:
            if now_time < trigger.trigger_time:
                continue  # not yet due today -- nothing missed
            if trigger.catch_up is TriggerCatchUpPolicy.SKIP:
                schedule.skipped_on[trigger.name] = today
                self._logger.warning(
                    "missed trigger %r for %s (due %s, now %s); skipping for "
                    "today per SKIP catch-up policy",
                    trigger.name, schedule.runner.identity_str, trigger.trigger_time, now_time,
                )
            else:
                # FIRE_ON_STARTUP: leave un-fired/un-skipped so the very next
                # tick's normal due-check fires it immediately.
                self._logger.warning(
                    "missed trigger %r for %s (due %s, now %s); firing "
                    "immediately per FIRE_ON_STARTUP catch-up policy",
                    trigger.name, schedule.runner.identity_str, trigger.trigger_time, now_time,
                )

    # -- Background loop -------------------------------------------------

    def start(self) -> None:
        """Start the background scheduling thread. Idempotent: a no-op if
        already running. Registering runners works before or after this call."""
        with self._lock:
            if self._thread is not None:
                return
            self._stop_event.clear()
            thread = threading.Thread(target=self._run_loop, name="PlatformScheduler", daemon=True)
            self._thread = thread
        thread.start()
        self._logger.info("scheduler started (poll_interval=%.2fs)", self._config.poll_interval_seconds)

    def stop(self) -> None:
        """Graceful shutdown: stop the background loop, then stop every
        currently-registered runner (each runner's own ``on_shutdown()`` hook
        runs via ``StrategyRunner.stop()``) and clear the schedule table --
        completing the lifecycle ``register()`` began and leaving this
        scheduler instance empty and reusable for a subsequent ``start()``/
        ``register()`` cycle (e.g. the dependency container restarting the
        platform without discarding the scheduler object). Idempotent."""
        with self._lock:
            thread = self._thread
            self._thread = None
            self._stop_event.set()
        if thread is not None:
            thread.join(timeout=self._config.poll_interval_seconds + 5.0)
            if thread.is_alive():
                self._logger.critical("scheduler background thread did not stop within the timeout")

        with self._lock:
            schedules = list(self._schedules.values())
        for schedule in schedules:
            try:
                schedule.runner.stop()
            except Exception:  # noqa: BLE001 -- one runner's shutdown failure must not block the rest
                self._logger.error(
                    "runner.stop() raised for %s during scheduler shutdown",
                    schedule.runner.identity_str, exc_info=True,
                )
        with self._lock:
            self._schedules.clear()
        self._logger.info("scheduler stopped")

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception:  # noqa: BLE001 -- the loop itself must survive an unexpected failure
                self._logger.critical("scheduler tick raised unexpectedly", exc_info=True)
            self._stop_event.wait(self._config.poll_interval_seconds)

    # -- One scheduling pass (safe to call directly in tests) -----------

    def _tick(self) -> None:
        """Evaluate every registered schedule once and dispatch whatever is
        due. Deliberately synchronous (it blocks until every dispatch this tick
        started has finished) so tests can call it directly for deterministic
        assertions and at-most-once-per-day holds -- but dispatches that fire in
        the *same* tick run concurrently (see ``_dispatch_concurrently``)."""
        today = self._time.today()
        now_time = self._time.now_ist().time()
        trading_day = self._calendar.is_trading_day(today)

        with self._lock:
            schedules = list(self._schedules.values())

        due: list[tuple[_RunnerSchedule, TimeTrigger]] = []
        for schedule in schedules:
            if schedule.runner.status is not RunnerStatus.RUNNING:
                continue
            if not trading_day:
                continue
            for trigger in schedule.triggers:
                if schedule.fired_on.get(trigger.name) == today:
                    continue
                if schedule.skipped_on.get(trigger.name) == today:
                    continue
                if now_time >= trigger.trigger_time:
                    due.append((schedule, trigger))

        # Mark every due trigger fired BEFORE dispatching any of them. This
        # guarantees at-most-once-per-day delivery independently of dispatch
        # order or timing, and this tick will not run again (nor will the next
        # one start) until the dispatches below all return.
        for schedule, trigger in due:
            schedule.fired_on[trigger.name] = today
            self._logger.info(
                "firing trigger %r for %s (due %s, fired %s)",
                trigger.name, schedule.runner.identity_str, trigger.trigger_time, now_time,
            )
        self._dispatch_concurrently(due)

    def _dispatch_concurrently(self, due: list[tuple[_RunnerSchedule, TimeTrigger]]) -> None:
        """Dispatch each due trigger, concurrently when more than one is due.

        Resolves the serial-dispatch skew (M1): when two instruments' trigger
        times coincide -- e.g. Nifty and Sensex both entering at 09:20 -- the
        second no longer waits for the first's entire fill-confirmation before
        it even begins. Each dispatch runs on its own thread and this method
        blocks until all have finished, so the tick stays synchronous and
        at-most-once-per-day is preserved. A single due trigger is dispatched
        inline (no thread), keeping the common case simple. Per-instance safety
        is unchanged: each runner still serialises its own hooks behind its own
        lock, so the concurrency here is strictly *across* instances.
        """
        if not due:
            return
        if len(due) == 1:
            schedule, trigger = due[0]
            self._dispatch_one(schedule, trigger)
            return
        threads = [
            threading.Thread(
                target=self._dispatch_one, args=(schedule, trigger),
                name=f"PlatformScheduler-dispatch-{schedule.runner.identity_str}", daemon=True,
            )
            for schedule, trigger in due
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

    def _dispatch_one(self, schedule: _RunnerSchedule, trigger: TimeTrigger) -> None:
        try:
            schedule.runner.dispatch_time_trigger(trigger.name)
        except Exception:  # noqa: BLE001 -- one runner's failure must not affect the others
            self._logger.critical(
                "dispatch_time_trigger raised for %s trigger %r (StrategyRunner "
                "should have already contained this -- investigate)",
                schedule.runner.identity_str, trigger.name, exc_info=True,
            )
