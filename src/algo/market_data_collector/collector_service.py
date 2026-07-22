"""CollectorService: the orchestrator that wires the collector together and runs
its control loop.

Threads in play: the KiteTicker's own socket thread (delivers ticks), the
``TickWriter``'s writer thread (persists batches), and one **controller** thread
here that drives the market-hours session and the ATM recompute/metrics cadences.

The tick handler does nothing but count + enqueue -- the socket thread never
touches the database. Session transitions (connect/collect/freeze/flush/
disconnect) are driven off ``MarketHoursController.phase`` so the collector
starts and stops itself with no manual intervention.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

from sqlalchemy import select

from algo.market_data_collector.db import collector_instruments
from algo.market_data_collector.market_hours import Phase

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy import Engine

    from algo.market_data_collector.atm_window import AtmWindowManager, InstrumentRef
    from algo.market_data_collector.config import CollectorConfig
    from algo.market_data_collector.full_tick_stream import CollectorTickStream
    from algo.market_data_collector.market_hours import MarketHoursController
    from algo.market_data_collector.metrics import CollectorMetrics
    from algo.market_data_collector.tick_writer import TickWriter
    from algo.strategy_engine.strategy_context import TimeProvider

_logger = logging.getLogger("algo.collector")


class CollectorService:
    def __init__(
        self,
        *,
        config: CollectorConfig,
        engine: Engine,
        stream: CollectorTickStream,
        writer: TickWriter,
        atm: AtmWindowManager,
        metrics: CollectorMetrics,
        market_hours: MarketHoursController,
        time_provider: TimeProvider,
        logger: logging.Logger | None = None,
    ) -> None:
        self._cfg = config
        self._engine = engine
        self._stream = stream
        self._writer = writer
        self._atm = atm
        self._metrics = metrics
        self._hours = market_hours
        self._time = time_provider
        self._logger = logger or _logger

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._phase: Phase | None = None
        self._last_recompute = 0.0
        self._last_metrics = 0.0

        # -- Watchdog state (monotonic seconds) --
        self._disconnected_since: float | None = None  # when is_connected() first went False
        self._connected_since: float | None = None     # when the current socket came up
        self._last_restart = 0.0                        # last forced rebuild (for backoff)
        self._hb_level = 0                              # 0 ok / 1 warned / 2 critical (rising-edge log)

    # -- Lifecycle -----------------------------------------------------

    def start(self) -> None:
        self._writer.start()
        self._thread = threading.Thread(target=self._run, name="collector-controller", daemon=True)
        self._thread.start()
        self._logger.info(
            "collector started: %d underlyings, ±%d strikes (%d instruments/underlying), mode=%s",
            len(self._cfg.underlyings), self._cfg.strikes_each_side,
            self._cfg.instruments_per_underlying, self._cfg.tick_mode,
        )

    def request_stop(self) -> None:
        """Signal-handler-safe stop request: wakes run_forever, which then does
        the real (thread-joining) shutdown."""
        self._stop.set()

    def stop(self) -> None:
        self._stop.set()
        t = self._thread
        self._thread = None
        if t is not None:
            t.join(timeout=15)
        self._stream.stop()
        self._writer.stop(drain=True)  # final drain to the DB
        self._logger.info("collector stopped")

    def run_forever(self) -> None:
        """Block until stop() is called (from a signal handler)."""
        self.start()
        try:
            while not self._stop.wait(timeout=1.0):
                pass
        finally:
            self.stop()

    # -- Controller loop -----------------------------------------------

    def _run(self) -> None:
        import time as _time
        poll = min(self._cfg.market_hours.idle_poll_seconds, 1.0)
        while not self._stop.is_set():
            try:
                now = self._time.now_ist()
                phase = self._hours.phase(now)
                self._on_phase(phase)
                mono = _time.monotonic()
                self._watchdog(phase, mono)
                if phase is Phase.COLLECT and mono - self._last_recompute >= self._cfg.recompute_interval_seconds:
                    self._recompute_all(now)
                    self._last_recompute = mono
                if mono - self._last_metrics >= self._cfg.metrics.interval_seconds:
                    self._logger.info("\n%s", self._metrics.render())
                    self._last_metrics = mono
            except Exception:  # noqa: BLE001 -- controller must never die
                self._logger.error("collector control loop error", exc_info=True)
            self._stop.wait(timeout=poll)

    def _on_phase(self, phase: Phase) -> None:
        if phase is self._phase:
            return
        prev, self._phase = self._phase, phase
        self._logger.info("collector phase: %s -> %s", prev.value if prev else "—", phase.value)
        if phase is Phase.CONNECT:
            # New session: fresh socket + fresh window state.
            self._atm.reset()
            self._stream.start()
        elif phase is Phase.COLLECT:
            self._last_recompute = 0.0  # force an immediate recompute this loop
        elif phase in (Phase.CLOSED, Phase.PRE_OPEN):
            # End of session (or day rolled over): disconnect + forget window.
            self._stream.stop()
            self._atm.reset()
        # FREEZE: stop recomputing (no action); FLUSH: writer keeps draining.

    # -- Self-healing watchdog -----------------------------------------

    def _watchdog(self, phase: Phase, mono: float) -> None:
        """Supervise the websocket from inside the controller loop.

        KiteTicker's own reconnect gets ``grace_seconds`` to recover a drop. If
        it can't -- or it permanently gives up (``on_noreconnect`` -> is_dead) --
        the watchdog destroys the dead socket and builds a brand-new one. A
        connected-but-silent socket (frozen reactor) is caught by the heartbeat.
        """
        rc = self._cfg.reconnect
        if phase not in (Phase.CONNECT, Phase.COLLECT, Phase.FREEZE):
            # Socket isn't expected up (pre-open / flush / closed): reset state so
            # the next session's watchdog starts from a clean slate.
            self._disconnected_since = self._connected_since = None
            self._hb_level = 0
            return

        dead = self._stream.is_dead()
        connected = self._stream.is_connected()

        if connected and not dead:
            if self._connected_since is None:
                if self._disconnected_since is not None:
                    self._logger.info("collector tick stream restored (connected)")
                self._connected_since = mono
                self._hb_level = 0
            self._disconnected_since = None
            if phase is Phase.COLLECT and self._heartbeat_stalled(mono, rc):
                self._restart_stream(mono, reason="heartbeat critical (frozen reactor)")
            return

        # Unhealthy: disconnected, or KiteTicker gave up reconnecting.
        self._connected_since = None
        if self._disconnected_since is None:
            self._disconnected_since = mono
            self._logger.info("collector network disconnected; awaiting reconnect")
        down_for = mono - self._disconnected_since

        if not (dead or down_for >= rc.grace_seconds):
            return  # inside the grace window -- let KiteTicker keep trying
        if mono - self._last_restart < rc.restart_backoff_seconds:
            return  # honour restart backoff; retry next iteration
        reason = "on_noreconnect (gave up)" if dead else (
            f"disconnected {down_for:.0f}s >= grace {rc.grace_seconds:.0f}s"
        )
        self._logger.warning("collector reconnect unsuccessful; watchdog taking over")
        self._restart_stream(mono, reason=reason)

    def _heartbeat_stalled(self, mono: float, rc) -> bool:  # noqa: ANN001 -- ReconnectConfig
        """Connected but no ticks: WARNING then CRITICAL (rising-edge logs).
        Returns True at the CRITICAL threshold so the caller forces a rebuild."""
        baseline = self._metrics.last_tick_at
        if baseline <= 0.0:  # no tick yet -> measure from when the socket came up
            baseline = self._connected_since if self._connected_since is not None else mono
        idle = mono - baseline
        if idle >= rc.heartbeat_critical_seconds:
            if self._hb_level < 2:
                self._logger.critical(
                    "collector connected but no tick for %.0fs (frozen reactor?)", idle
                )
                self._hb_level = 2
            return True
        if idle >= rc.heartbeat_warning_seconds:
            if self._hb_level < 1:
                self._logger.warning("collector connected but no tick for %.0fs", idle)
                self._hb_level = 1
            return False
        self._hb_level = 0
        return False

    def _restart_stream(self, mono: float, *, reason: str) -> None:
        """Destroy the dead websocket and bring up a brand-new KiteTicker.
        Never restarts the process. Subscriptions are restored automatically by
        the fresh socket's on_connect (the intended set is preserved)."""
        self._logger.critical("watchdog restarting websocket: %s", reason)
        self._stream.restart()
        self._metrics.on_restart()
        self._last_restart = mono
        # Fresh socket gets its own grace + heartbeat baseline once it connects;
        # force an ATM recompute next COLLECT loop so edge rotation resumes.
        self._disconnected_since = self._connected_since = None
        self._hb_level = 0
        self._last_recompute = 0.0

    def _recompute_all(self, now: datetime) -> None:
        today = now.date()
        for underlying in self._cfg.underlyings:
            try:
                diff = self._atm.recompute(underlying, today)
            except Exception:  # noqa: BLE001 -- one underlying's failure must not stop the rest
                self._logger.error("ATM recompute failed for %s", underlying, exc_info=True)
                continue
            if diff.new_instruments:
                self._upsert_instruments(diff.new_instruments, now)
            if diff.to_unsubscribe:
                self._stream.unsubscribe(diff.to_unsubscribe)
            if diff.to_subscribe:
                self._stream.subscribe(diff.to_subscribe)
            if diff.to_subscribe or diff.to_unsubscribe:
                self._logger.info(
                    "%s ATM=%s: +%d / -%d instruments (window ±%d strikes)",
                    underlying, diff.atm, len(diff.to_subscribe), len(diff.to_unsubscribe),
                    self._cfg.strikes_each_side,
                )

    def _upsert_instruments(self, refs: list[InstrumentRef], now: datetime) -> None:
        """Insert dimension rows for newly-seen tokens (idempotent by PK)."""
        tokens = [r.instrument_token for r in refs]
        with self._engine.begin() as conn:
            existing = set(conn.execute(
                select(collector_instruments.c.instrument_token).where(
                    collector_instruments.c.instrument_token.in_(tokens)
                )
            ).scalars())
            new_rows = [
                {
                    "instrument_token": r.instrument_token, "underlying": r.underlying,
                    "exchange": r.exchange.value, "expiry_date": r.expiry, "strike": r.strike,
                    "option_type": r.option_type.value, "tradingsymbol": r.tradingsymbol,
                    "first_seen": now,
                }
                for r in refs if r.instrument_token not in existing
            ]
            if new_rows:
                conn.execute(collector_instruments.insert(), new_rows)
