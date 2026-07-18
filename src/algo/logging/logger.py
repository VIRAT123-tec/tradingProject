"""Central logging configuration (resolves the structured-logging half of M3).

``configure_logging`` replaces the bare ``logging.basicConfig(level=INFO)`` the
entrypoints used with a consistent, timestamped format, and -- when given an
``AlertDispatcher`` -- attaches an :class:`AlertingHandler` so CRITICAL events
are surfaced to a human rather than only written to the log stream.

It is deliberately small and dependency-injected: the entrypoint owns the log
level and the alert dispatcher and passes them in, so the whole thing is
testable and nothing is a hidden global. Idempotent -- safe to call once at
startup (it resets the root handlers rather than stacking duplicates).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from algo.logging.alerting import AlertingHandler

if TYPE_CHECKING:
    from algo.logging.alerting import AlertDispatcher

_DEFAULT_FORMAT = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"


def configure_logging(
    level: int | str = logging.INFO,
    *,
    alert_dispatcher: AlertDispatcher | None = None,
    log_format: str = _DEFAULT_FORMAT,
    stream_handler: logging.Handler | None = None,
) -> None:
    """Configure process-wide logging.

    ``level`` sets the root threshold. ``alert_dispatcher``, if given, receives
    every CRITICAL-and-above record (attached to the ``algo`` logger, so it
    catches the platform's own critical events without also firing on unrelated
    library noise). ``stream_handler`` overrides the default stderr handler
    (used by tests to capture output). Calling this again replaces the handlers
    rather than adding to them.
    """
    formatter = logging.Formatter(log_format)

    root = logging.getLogger()
    root.setLevel(level)
    # Reset handlers so a second call does not stack duplicates.
    for handler in list(root.handlers):
        root.removeHandler(handler)
    base = stream_handler if stream_handler is not None else logging.StreamHandler()
    base.setFormatter(formatter)
    root.addHandler(base)

    # The platform logs everything under 'algo.*'; keep it at the chosen level.
    logging.getLogger("algo").setLevel(level)

    if alert_dispatcher is not None:
        algo_logger = logging.getLogger("algo")
        # Avoid stacking multiple alerting handlers across repeated calls.
        for handler in list(algo_logger.handlers):
            if isinstance(handler, AlertingHandler):
                algo_logger.removeHandler(handler)
        algo_logger.addHandler(AlertingHandler(alert_dispatcher))
