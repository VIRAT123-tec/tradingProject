"""Alerting: surface CRITICAL-level events (frozen instruments, reconciliation
breaks, broker/DB divergences) to a human, instead of leaving them buried in
the log stream (resolves the "nothing surfaces critical events" half of M3).

This is deliberately just the *mechanism*, not a specific external integration.
An ``AlertingHandler`` is a standard ``logging.Handler`` that forwards every
record at or above a threshold (CRITICAL by default) to an injected
``AlertDispatcher``. The default ``RecordingAlertDispatcher`` keeps a bounded
in-memory history (so a health/status surface can show "have there been any
alerts?") and re-emits each alert on a dedicated ``algo.alerts`` logger for
visibility. A real channel (webhook, email, Slack, PagerDuty) is a drop-in
``AlertDispatcher`` implementation added later -- building one is out of scope
here.

Thread-safety and safety: the handler is invoked from any thread (a strategy
worker, the websocket thread, the scheduler); the recording dispatcher guards
its history with a lock, and the handler never lets a dispatch failure escape
(a logging handler that raises would break logging itself). Re-emission is at
WARNING -- below the CRITICAL threshold -- so it can never re-trigger the
handler recursively.
"""

from __future__ import annotations

import logging
import threading
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class AlertEvent:
    """One alert-worthy event, distilled from a log record."""

    level: str
    logger_name: str
    message: str
    created_at: datetime


@runtime_checkable
class AlertDispatcher(Protocol):
    """Seam for delivering an alert somewhere a human will see it. A real
    implementation might POST to a webhook or send an email; the platform
    depends only on this method."""

    def dispatch(self, event: AlertEvent) -> None: ...


class RecordingAlertDispatcher:
    """Default dispatcher: keeps a bounded history of recent alerts and
    re-emits each on the ``algo.alerts`` logger.

    The history lets an operator/health surface answer "has anything gone
    critically wrong?" without scraping the whole log. Re-emitting on a distinct
    logger makes alerts easy to route or grep separately from ordinary logs.
    """

    def __init__(self, *, max_history: int = 256) -> None:
        self._lock = threading.Lock()
        self._history: deque[AlertEvent] = deque(maxlen=max_history)
        self._alert_logger = logging.getLogger("algo.alerts")

    def dispatch(self, event: AlertEvent) -> None:
        with self._lock:
            self._history.append(event)
        # WARNING (below the CRITICAL alert threshold) so this cannot recurse
        # back into the AlertingHandler.
        self._alert_logger.warning("ALERT [%s] %s: %s", event.level, event.logger_name, event.message)

    def recent(self) -> list[AlertEvent]:
        """A snapshot of the recent alert history (newest last)."""
        with self._lock:
            return list(self._history)

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._history)


class AlertingHandler(logging.Handler):
    """A logging handler that forwards records at/above ``threshold`` to an
    ``AlertDispatcher``."""

    def __init__(self, dispatcher: AlertDispatcher, *, threshold: int = logging.CRITICAL) -> None:
        super().__init__(level=threshold)
        self._dispatcher = dispatcher

    def emit(self, record: logging.LogRecord) -> None:
        # Called from arbitrary threads; must never raise.
        try:
            event = AlertEvent(
                level=record.levelname,
                logger_name=record.name,
                message=record.getMessage(),
                created_at=datetime.fromtimestamp(record.created, tz=timezone.utc),
            )
            self._dispatcher.dispatch(event)
        except Exception:  # noqa: BLE001 -- a handler that raises would break logging
            self.handleError(record)
