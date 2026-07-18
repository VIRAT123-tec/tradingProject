"""Application bootstrap: the OS-process layer sitting on top of
``DependencyContainer`` (``dependency_container.py``).

The container already does the real work this module's requirements describe
-- loading configuration, connecting the database, initializing the broker,
market data, risk core, reconciliation engine, and scheduler, and registering
every configured strategy instance (``DependencyContainer.__init__`` +
``start()``, reviewed and approved in Task 24). What a *process* needs on top
of that, and all this module adds, is: install OS signal handling, block the
main thread until asked to stop, and shut down cleanly -- including cleanly
unwinding a *failed* startup, so a broker-auth failure halfway through never
leaves a websocket thread or scheduler running behind it.

``start_paper.py`` and ``start_live.py`` are the two real process entrypoints
(``python start_paper.py`` / ``python start_live.py``); this module holds the
logic shared between them in one testable class so neither script duplicates
signal handling or shutdown ordering.
"""

from __future__ import annotations

import logging
import signal
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from algo.common.enums import BrokerName
    from algo.dependency_container import DependencyContainer


class BrokerModeMismatchError(RuntimeError):
    """Raised when the running entrypoint's expected broker mode does not
    match ``brokers.yaml``'s ``active_broker`` -- e.g. ``start_live.py`` was
    run while the config still selects SIMULATION, or vice versa.

    A real-money trading platform must never depend on an operator
    remembering to keep two independent surfaces (which script they ran, and
    what the config file says) in sync; refusing to start is the safe failure
    mode here, not a warning.
    """


class Application:
    """Runs an already-constructed ``DependencyContainer`` as a long-lived OS
    process.

    Deliberately thin: this class never builds the container itself (the
    caller does -- see ``start_paper.py``/``start_live.py``, which is also
    where "avoid global state" is honored: nothing here is a module-level
    singleton, every dependency is a constructor or method argument) and its
    ``start()``/``stop()`` call straight through to the container's own
    lifecycle methods. The only things it adds are process concerns the
    container itself has no business knowing about: OS signal handling and
    blocking the main thread until asked to stop.
    """

    def __init__(
        self,
        container: DependencyContainer,
        *,
        expected_broker: BrokerName | None = None,
        install_signal_handlers: bool = True,
        logger: logging.Logger | None = None,
    ) -> None:
        """``container`` must already be fully constructed (per
        ``DependencyContainer``'s own two-phase design: config loaded and
        validated, every service wired) but not yet started -- this class
        never mutates construction-time state, only lifecycle state.

        ``expected_broker``, if given, is checked against
        ``container.brokers_config.active_broker`` before starting: the
        entrypoint that knows which mode it is meant for (paper vs. live)
        should always pass this, so a config/script mismatch fails loudly at
        startup instead of silently running the wrong broker.

        ``install_signal_handlers`` defaults to ``True`` for real process use;
        a test sets it ``False`` and calls ``request_shutdown()`` directly
        instead, both because ``signal.signal()`` only works on the main
        thread (pytest workers are not always the main thread) and because a
        test process should never have its own signal handling overridden.
        """
        self._container = container
        self._expected_broker = expected_broker
        self._install_signal_handlers_flag = install_signal_handlers
        self._logger = logger if logger is not None else logging.getLogger("algo.app")
        self._shutdown_event = threading.Event()
        self._signal_handlers_installed = False

    # -- Full lifecycle, the entrypoint scripts' only call ------------------

    def run(self) -> None:
        """Verify broker mode, start the container, block until a shutdown
        signal arrives, then stop the container. Equivalent to
        ``start()``, ``wait_for_shutdown()``, ``stop()`` in sequence, with
        ``stop()`` guaranteed to run even if ``wait_for_shutdown()`` is
        interrupted unexpectedly.
        """
        self.start()
        try:
            self.wait_for_shutdown()
        finally:
            self.stop()

    # -- Individual lifecycle steps (exposed for tests and finer control) --

    def start(self) -> None:
        """Verify broker mode, then start the container.

        Handles startup failure cleanly: if ``expected_broker`` does not
        match, the container is never started at all (nothing to tear down).
        If ``container.start()`` itself raises partway through (e.g. broker
        authentication fails after market data has already connected), this
        stops the container before re-raising, so a failed startup never
        leaves a broker connection, market data thread, or scheduler running
        behind it.
        """
        if self._expected_broker is not None:
            actual = self._container.brokers_config.active_broker
            if actual is not self._expected_broker:
                raise BrokerModeMismatchError(
                    f"expected active_broker={self._expected_broker.value}, but "
                    f"brokers.yaml selects {actual.value}"
                )

        if self._install_signal_handlers_flag:
            self._install_signal_handlers()

        try:
            self._container.start()
        except Exception:
            self._logger.critical("startup failed; tearing down any partial state", exc_info=True)
            self._container.stop()
            raise
        self._logger.info("application started")

    def wait_for_shutdown(self, *, timeout: float | None = None) -> None:
        """Block the calling thread until a shutdown is requested -- by an
        installed OS signal handler, by a direct ``request_shutdown()`` call
        (e.g. from a test), or by ``KeyboardInterrupt`` (handled here as a
        fallback graceful-shutdown trigger even when
        ``install_signal_handlers=False``, so Ctrl+C during a test never
        leaves a stray background thread running).

        ``timeout`` is ``None`` (block forever) for real process use; a test
        passes a short timeout as a safety net against hanging forever if
        nothing ever signals shutdown.
        """
        try:
            self._shutdown_event.wait(timeout)
        except KeyboardInterrupt:
            self._logger.info("KeyboardInterrupt received; requesting graceful shutdown")
            self.request_shutdown()

    def request_shutdown(self) -> None:
        """Ask ``wait_for_shutdown()`` to return. Safe to call from a signal
        handler, another thread, or a test."""
        self._shutdown_event.set()

    def stop(self) -> None:
        """Stop the container. Idempotent -- delegates to
        ``DependencyContainer.stop()``, which is itself idempotent, so this
        is safe to call more than once (e.g. once from ``run()``'s
        ``finally`` and once more explicitly by a test)."""
        self._container.stop()
        self._logger.info("application stopped")

    # -- Internal -------------------------------------------------------

    def _install_signal_handlers(self) -> None:
        if self._signal_handlers_installed:
            return

        def _handle(signum: int, frame: object) -> None:  # noqa: ARG001
            self._logger.info("received signal %s; requesting graceful shutdown", signum)
            self.request_shutdown()

        signal.signal(signal.SIGINT, _handle)
        signal.signal(signal.SIGTERM, _handle)
        self._signal_handlers_installed = True
