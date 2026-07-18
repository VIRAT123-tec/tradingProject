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
