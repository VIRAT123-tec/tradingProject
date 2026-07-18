"""Tests for the scheduler's concurrent same-tick dispatch (M1).

Uses a lightweight fake runner so the scheduler's dispatch behaviour is tested
in isolation. Concurrency is proven deterministically with a barrier: two
runners whose triggers fire in the same tick must BOTH be inside their dispatch
at once for the barrier to release. If dispatch were serial, the first would
wait on a barrier party that never arrives and the barrier would break.
"""

from __future__ import annotations

import threading
from datetime import date, datetime, time, timezone

from algo.scheduler import PlatformScheduler
from algo.strategy_engine.strategy_base import TimeTrigger, TriggerCatchUpPolicy
from algo.strategy_engine.strategy_runner import RunnerStatus


class MutableTime:
    def __init__(self, hour, minute, day=date(2026, 7, 8)):
        self.ist = datetime(day.year, day.month, day.day, hour, minute, tzinfo=timezone.utc)

    def now(self):
        return self.ist

    def now_ist(self):
        return self.ist

    def today(self):
        return self.ist.date()


class FakeRunner:
    def __init__(self, identity, triggers, *, on_dispatch=None):
        self._identity = identity
        self._triggers = triggers
        self._on_dispatch = on_dispatch
        self.status = RunnerStatus.RUNNING
        self.dispatched: list[str] = []
        self.started = False
        self.stopped = False

    @property
    def identity_str(self):
        return self._identity

    def scheduled_triggers(self):
        return self._triggers

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True

    def dispatch_time_trigger(self, name):
        self.dispatched.append(name)
        if self._on_dispatch is not None:
            self._on_dispatch()


def _entry_trigger():
    return [TimeTrigger("entry", time(9, 20), TriggerCatchUpPolicy.SKIP)]


class TestConcurrentDispatch:
    def test_two_coincident_triggers_dispatch_concurrently(self):
        # A barrier for two parties: both dispatches must be in-flight at once.
        barrier = threading.Barrier(2)

        def hit_barrier():
            barrier.wait(timeout=3.0)

        clock = MutableTime(9, 0)
        r1 = FakeRunner("a", _entry_trigger(), on_dispatch=hit_barrier)
        r2 = FakeRunner("b", _entry_trigger(), on_dispatch=hit_barrier)
        sched = PlatformScheduler(time_provider=clock)
        sched.register(r1)
        sched.register(r2)

        clock.ist = clock.ist.replace(hour=9, minute=20)
        sched._tick()  # noqa: SLF001 -- both fire this tick

        assert r1.dispatched == ["entry"]
        assert r2.dispatched == ["entry"]
        # If dispatch had been serial, the first dispatch's barrier.wait would
        # have timed out (only one party) and broken the barrier.
        assert barrier.broken is False

    def test_single_due_trigger_dispatches_inline(self):
        clock = MutableTime(9, 0)
        r = FakeRunner("solo", _entry_trigger())
        sched = PlatformScheduler(time_provider=clock)
        sched.register(r)

        clock.ist = clock.ist.replace(hour=9, minute=20)
        sched._tick()  # noqa: SLF001

        assert r.dispatched == ["entry"]

    def test_at_most_once_holds_across_concurrent_dispatch(self):
        clock = MutableTime(9, 0)
        r1 = FakeRunner("a", _entry_trigger())
        r2 = FakeRunner("b", _entry_trigger())
        sched = PlatformScheduler(time_provider=clock)
        sched.register(r1)
        sched.register(r2)

        clock.ist = clock.ist.replace(hour=9, minute=20)
        sched._tick()  # noqa: SLF001
        sched._tick()  # noqa: SLF001 -- second tick must NOT re-dispatch
        sched._tick()  # noqa: SLF001

        assert r1.dispatched == ["entry"]  # exactly once
        assert r2.dispatched == ["entry"]

    def test_one_dispatch_failure_does_not_block_the_other(self):
        def boom():
            raise RuntimeError("dispatch blew up")

        clock = MutableTime(9, 0)
        bad = FakeRunner("bad", _entry_trigger(), on_dispatch=boom)
        good = FakeRunner("good", _entry_trigger())
        sched = PlatformScheduler(time_provider=clock)
        sched.register(bad)
        sched.register(good)

        clock.ist = clock.ist.replace(hour=9, minute=20)
        sched._tick()  # noqa: SLF001 -- must not raise despite bad runner

        assert good.dispatched == ["entry"]
