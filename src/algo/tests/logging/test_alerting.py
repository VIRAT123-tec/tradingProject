"""Tests for the alerting mechanism and logging setup (M3)."""

from __future__ import annotations

import logging
import threading

import pytest

from algo.logging.alerting import (
    AlertEvent,
    AlertingHandler,
    RecordingAlertDispatcher,
)
from algo.logging.logger import configure_logging


@pytest.fixture
def _restore_logging():
    """Snapshot and restore the (process-global) logging config, so a test that
    calls configure_logging does not leak handlers into other tests."""
    root = logging.getLogger()
    algo = logging.getLogger("algo")
    saved = (list(root.handlers), root.level, list(algo.handlers), algo.level)
    try:
        yield
    finally:
        root.handlers[:] = saved[0]
        root.setLevel(saved[1])
        algo.handlers[:] = saved[2]
        algo.setLevel(saved[3])


class TestRecordingAlertDispatcher:
    def test_records_and_exposes_recent(self):
        d = RecordingAlertDispatcher()
        d.dispatch(AlertEvent(level="CRITICAL", logger_name="algo.x", message="boom",
                              created_at=__import__("datetime").datetime.now()))
        assert d.count == 1
        assert d.recent()[-1].message == "boom"

    def test_history_is_bounded(self):
        d = RecordingAlertDispatcher(max_history=3)
        import datetime
        for i in range(10):
            d.dispatch(AlertEvent(level="CRITICAL", logger_name="algo", message=str(i),
                                  created_at=datetime.datetime.now()))
        assert d.count == 3
        assert [e.message for e in d.recent()] == ["7", "8", "9"]

    def test_thread_safe_under_concurrent_dispatch(self):
        d = RecordingAlertDispatcher(max_history=1000)
        import datetime

        def worker():
            for _ in range(100):
                d.dispatch(AlertEvent(level="CRITICAL", logger_name="algo", message="x",
                                      created_at=datetime.datetime.now()))

        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert d.count == 500


class TestAlertingHandler:
    def test_forwards_critical_records(self):
        d = RecordingAlertDispatcher()
        handler = AlertingHandler(d)
        logger = logging.getLogger("test.alerting.crit")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        try:
            logger.critical("something critical")
            assert d.count == 1
            assert d.recent()[-1].message == "something critical"
        finally:
            logger.removeHandler(handler)

    def test_ignores_below_threshold(self):
        d = RecordingAlertDispatcher()
        handler = AlertingHandler(d)
        logger = logging.getLogger("test.alerting.warn")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        try:
            logger.warning("just a warning")
            logger.error("an error")
            assert d.count == 0  # only CRITICAL alerts
        finally:
            logger.removeHandler(handler)

    def test_handler_never_raises_if_dispatcher_raises(self):
        class BoomDispatcher:
            def dispatch(self, event):  # noqa: ANN001, ARG002
                raise RuntimeError("dispatch failed")

        handler = AlertingHandler(BoomDispatcher())
        logger = logging.getLogger("test.alerting.boom")
        logger.addHandler(handler)
        try:
            logger.critical("boom")  # must not propagate the dispatcher error
        finally:
            logger.removeHandler(handler)


class TestConfigureLogging:
    def test_critical_algo_log_triggers_the_dispatcher(self, _restore_logging):
        d = RecordingAlertDispatcher()
        configure_logging(logging.INFO, alert_dispatcher=d)

        logging.getLogger("algo.some.module").critical("frozen instance")

        assert d.count == 1

    def test_no_recursion_from_the_re_emitted_alert(self, _restore_logging):
        # The dispatcher re-emits at WARNING on algo.alerts; that must not
        # re-trigger the CRITICAL AlertingHandler.
        d = RecordingAlertDispatcher()
        configure_logging(logging.INFO, alert_dispatcher=d)

        logging.getLogger("algo.x").critical("one critical event")

        assert d.count == 1  # exactly one, not two or infinite

    def test_repeated_calls_do_not_stack_handlers(self, _restore_logging):
        d = RecordingAlertDispatcher()
        configure_logging(logging.INFO, alert_dispatcher=d)
        configure_logging(logging.INFO, alert_dispatcher=d)

        logging.getLogger("algo.y").critical("event")

        assert d.count == 1  # not 2 -- the second configure replaced, not added
