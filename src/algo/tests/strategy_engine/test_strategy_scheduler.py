"""Unit tests for MonitoringScheduler.

Uses a lightweight FakeRunner (this scheduler only needs .status,
.identity_str, and .dispatch_monitor_cycle) so the scheduler's own behaviour --
dispatch to RUNNING runners, skip others, isolation, start/stop -- is exercised
in isolation from the full StrategyRunner. The end-to-end proof that a real
runner's monitor cycle actually fires an intraday exit lives in the integration
suite (test_intraday_monitoring.py).
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from algo.strategy_engine.strategy_runner import RunnerStatus
from algo.strategy_engine.strategy_scheduler import MonitoringScheduler, MonitoringSchedulerConfig


@dataclass
class FakeRunner:
    identity: str
    status: RunnerStatus = RunnerStatus.RUNNING
    calls: int = 0
    raise_on_dispatch: bool = False
    _lock: threading.Lock = field(default_factory=threading.Lock)

    @property
    def identity_str(self) -> str:
        return self.identity

    def dispatch_monitor_cycle(self) -> None:
        with self._lock:
            self.calls += 1
        if self.raise_on_dispatch:
            raise RuntimeError("boom in dispatch")


def _fast() -> MonitoringSchedulerConfig:
    return MonitoringSchedulerConfig(interval_seconds=0.02)


class TestTickDispatch:
    def test_tick_dispatches_to_running_runners(self):
        r1, r2 = FakeRunner("a"), FakeRunner("b")
        sched = MonitoringScheduler()
        sched.register(r1)
        sched.register(r2)

        sched._tick()  # noqa: SLF001

        assert r1.calls == 1
        assert r2.calls == 1

    def test_non_running_runner_is_skipped(self):
        running = FakeRunner("run", status=RunnerStatus.RUNNING)
        frozen = FakeRunner("frozen", status=RunnerStatus.FROZEN)
        stopped = FakeRunner("stopped", status=RunnerStatus.STOPPED)
        sched = MonitoringScheduler()
        for r in (running, frozen, stopped):
            sched.register(r)

        sched._tick()  # noqa: SLF001

        assert running.calls == 1
        assert frozen.calls == 0
        assert stopped.calls == 0

    def test_one_runner_failure_does_not_stop_the_others(self):
        bad = FakeRunner("bad", raise_on_dispatch=True)
        good = FakeRunner("good")
        sched = MonitoringScheduler()
        sched.register(bad)
        sched.register(good)

        sched._tick()  # must not raise despite bad runner  # noqa: SLF001

        assert good.calls == 1


class TestRegistration:
    def test_register_is_idempotent_per_identity(self):
        sched = MonitoringScheduler()
        r = FakeRunner("x")
        sched.register(r)
        sched.register(r)  # same identity again
        assert sched.registered_identities() == ["x"]

    def test_unregister_removes_the_runner(self):
        sched = MonitoringScheduler()
        r = FakeRunner("x")
        sched.register(r)
        sched.unregister(r)
        assert sched.registered_identities() == []
        sched._tick()  # noqa: SLF001
        assert r.calls == 0

    def test_unregister_unknown_is_noop(self):
        sched = MonitoringScheduler()
        sched.unregister(FakeRunner("never"))  # must not raise


class TestBackgroundThread:
    def test_thread_dispatches_repeatedly(self):
        r = FakeRunner("x")
        sched = MonitoringScheduler(config=_fast())
        sched.register(r)
        sched.start()
        try:
            deadline = time.monotonic() + 3.0
            while r.calls < 3 and time.monotonic() < deadline:
                time.sleep(0.01)
        finally:
            sched.stop()
        assert r.calls >= 3

    def test_start_is_idempotent(self):
        sched = MonitoringScheduler(config=_fast())
        sched.start()
        first = sched._thread  # noqa: SLF001
        sched.start()
        assert sched._thread is first  # noqa: SLF001
        sched.stop()

    def test_stop_is_idempotent_and_clears_registrations(self):
        sched = MonitoringScheduler(config=_fast())
        sched.register(FakeRunner("x"))
        sched.start()
        sched.stop()
        sched.stop()  # must not raise
        assert sched.registered_identities() == []

    def test_stop_leaves_it_reusable(self):
        sched = MonitoringScheduler(config=_fast())
        r1 = FakeRunner("x")
        sched.register(r1)
        sched.start()
        sched.stop()

        r2 = FakeRunner("y")
        sched.register(r2)
        sched.start()
        try:
            deadline = time.monotonic() + 3.0
            while r2.calls < 1 and time.monotonic() < deadline:
                time.sleep(0.01)
        finally:
            sched.stop()
        assert r2.calls >= 1


class TestConfigValidation:
    def test_interval_must_be_positive(self):
        import pytest

        with pytest.raises(Exception):
            MonitoringSchedulerConfig(interval_seconds=0)
