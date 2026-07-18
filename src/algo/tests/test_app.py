"""Tests for app.Application.

Uses a lightweight fake standing in for DependencyContainer (this module only
needs .start()/.stop()/.brokers_config.active_broker -- it never constructs
or introspects a real container), so these tests exercise Application's own
process-lifecycle logic (signal handling, shutdown blocking, startup-failure
cleanup) in isolation from container wiring, which is covered separately in
test_dependency_container.py.
"""

from __future__ import annotations

import logging
import signal
import threading
import time

import pytest

from algo.app import Application, BrokerModeMismatchError
from algo.common.enums import BrokerName


class FakeBrokersConfig:
    def __init__(self, active_broker: BrokerName) -> None:
        self.active_broker = active_broker


class FakeContainer:
    def __init__(self, *, active_broker: BrokerName = BrokerName.SIMULATION, fail_on_start: bool = False) -> None:
        self.brokers_config = FakeBrokersConfig(active_broker)
        self.start_calls = 0
        self.stop_calls = 0
        self._fail_on_start = fail_on_start

    def start(self) -> None:
        self.start_calls += 1
        if self._fail_on_start:
            raise RuntimeError("boom during container.start()")

    def stop(self) -> None:
        self.stop_calls += 1


class TestBrokerModeCheck:
    def test_matching_expected_broker_starts_the_container(self):
        container = FakeContainer(active_broker=BrokerName.SIMULATION)
        app = Application(container, expected_broker=BrokerName.SIMULATION, install_signal_handlers=False)

        app.start()

        assert container.start_calls == 1

    def test_mismatched_expected_broker_raises_and_never_starts(self):
        container = FakeContainer(active_broker=BrokerName.KITE)
        app = Application(container, expected_broker=BrokerName.SIMULATION, install_signal_handlers=False)

        with pytest.raises(BrokerModeMismatchError, match="SIMULATION"):
            app.start()

        assert container.start_calls == 0

    def test_no_expected_broker_skips_the_check(self):
        container = FakeContainer(active_broker=BrokerName.KITE)
        app = Application(container, install_signal_handlers=False)

        app.start()

        assert container.start_calls == 1


class TestStartupFailureHandling:
    def test_container_start_failure_stops_the_container_before_reraising(self):
        container = FakeContainer(fail_on_start=True)
        app = Application(container, install_signal_handlers=False)

        with pytest.raises(RuntimeError, match="boom"):
            app.start()

        assert container.start_calls == 1
        assert container.stop_calls == 1  # torn down, not left half-started


class TestShutdown:
    def test_stop_delegates_to_container_and_is_idempotent(self):
        container = FakeContainer()
        app = Application(container, install_signal_handlers=False)
        app.start()

        app.stop()
        app.stop()

        assert container.stop_calls == 2  # Application itself doesn't dedupe; container.stop() must

    def test_wait_for_shutdown_returns_once_requested(self):
        container = FakeContainer()
        app = Application(container, install_signal_handlers=False)
        app.start()

        app.request_shutdown()
        app.wait_for_shutdown(timeout=5.0)  # must return promptly, not block for the full timeout

    def test_wait_for_shutdown_returns_after_timeout_if_never_requested(self):
        container = FakeContainer()
        app = Application(container, install_signal_handlers=False)
        app.start()

        started = time.monotonic()
        app.wait_for_shutdown(timeout=0.1)
        elapsed = time.monotonic() - started

        assert elapsed < 2.0  # did not hang forever

    def test_request_shutdown_from_another_thread_unblocks_wait(self):
        container = FakeContainer()
        app = Application(container, install_signal_handlers=False)
        app.start()

        def _trigger():
            time.sleep(0.05)
            app.request_shutdown()

        threading.Thread(target=_trigger, daemon=True).start()

        started = time.monotonic()
        app.wait_for_shutdown(timeout=5.0)
        elapsed = time.monotonic() - started

        assert elapsed < 2.0

    def test_run_full_cycle(self):
        container = FakeContainer()
        app = Application(container, install_signal_handlers=False)
        app.request_shutdown()  # pre-armed: wait_for_shutdown returns immediately

        app.run()

        assert container.start_calls == 1
        assert container.stop_calls == 1

    def test_run_stops_container_even_if_wait_raises(self, monkeypatch):
        container = FakeContainer()
        app = Application(container, install_signal_handlers=False)

        def _explode(timeout=None):  # noqa: ARG001
            raise RuntimeError("unexpected failure while waiting")

        monkeypatch.setattr(app, "wait_for_shutdown", _explode)

        with pytest.raises(RuntimeError, match="unexpected failure"):
            app.run()

        assert container.stop_calls == 1


class TestKeyboardInterruptHandling:
    def test_keyboard_interrupt_during_wait_triggers_graceful_shutdown(self, monkeypatch):
        container = FakeContainer()
        app = Application(container, install_signal_handlers=False)
        app.start()

        def _raise_keyboard_interrupt(timeout=None):  # noqa: ARG001
            raise KeyboardInterrupt

        monkeypatch.setattr(app._shutdown_event, "wait", _raise_keyboard_interrupt)

        app.wait_for_shutdown()  # must not propagate KeyboardInterrupt

        assert app._shutdown_event.is_set()


class TestSignalHandlerInstallation:
    def test_install_signal_handlers_false_never_touches_signal_module(self, monkeypatch):
        def _explode(*args, **kwargs):
            raise AssertionError("signal.signal must not be called when install_signal_handlers=False")

        monkeypatch.setattr(signal, "signal", _explode)
        container = FakeContainer()
        app = Application(container, install_signal_handlers=False)

        app.start()  # must not raise

    def test_installed_handler_requests_shutdown(self, monkeypatch):
        captured: dict[int, object] = {}

        def _capture(signum, handler):
            captured[signum] = handler

        monkeypatch.setattr(signal, "signal", _capture)
        container = FakeContainer()
        app = Application(container, install_signal_handlers=True)

        app.start()

        assert signal.SIGINT in captured
        assert signal.SIGTERM in captured
        assert not app._shutdown_event.is_set()

        captured[signal.SIGINT](signal.SIGINT, None)  # simulate delivery without a real OS signal

        assert app._shutdown_event.is_set()

    def test_signal_handlers_installed_only_once(self, monkeypatch):
        calls = []

        def _capture(signum, handler):
            calls.append(signum)

        monkeypatch.setattr(signal, "signal", _capture)
        container = FakeContainer()
        app = Application(container, install_signal_handlers=True)

        app._install_signal_handlers()
        app._install_signal_handlers()

        assert calls == [signal.SIGINT, signal.SIGTERM]  # not doubled


class TestLoggerInjection:
    def test_custom_logger_is_used(self):
        container = FakeContainer()
        logger = logging.getLogger("test.custom")
        app = Application(container, install_signal_handlers=False, logger=logger)

        assert app._logger is logger
