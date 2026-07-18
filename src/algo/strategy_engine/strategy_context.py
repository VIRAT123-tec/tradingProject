"""StrategyContext: the dependency-injection bundle handed to every running
strategy, plus the minimal seam Protocols the engine depends on.

A concrete strategy never constructs its own broker, database session, market-
data feed, or clock -- it receives all of them, already wired, inside a single
frozen ``StrategyContext``. This is what keeps strategy code both strategy-
agnostic (it names only abstractions) and testable (a test injects fakes).

Seam Protocols
--------------
``MarketDataGateway``, ``RiskGateway``, and ``TimeProvider`` are defined here
as thin ``Protocol`` seams so the strategy engine is fully typed and testable
*before* the ``market_data/``, ``risk/``, and ``services/`` packages exist.
Those packages will later provide concrete implementations that structurally
satisfy these Protocols; nothing in the engine imports them directly. When a
package materializes it may widen its own class beyond the Protocol -- the
Protocol is the minimum the engine relies on, not the whole surface.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from algo.brokers.broker_base import BrokerBase, InstrumentIdentifier
from algo.common.enums import Exchange
from algo.services.expiry_service import ExpiryService
from algo.services.instrument_service import InstrumentService
from algo.services.pricing_service import SpotPriceProvider

if TYPE_CHECKING:
    from pydantic import BaseModel
    from sqlalchemy.orm import Session, sessionmaker


@dataclass(frozen=True, slots=True)
class StrategyIdentity:
    """The immutable identity of one running strategy instance.

    Sourced from the persistent ``StrategyInstance`` row and stamped onto
    every log line, database write, and alert so reconciliation can always
    tie an event back to exactly one (strategy, instrument, account).
    """

    instance_id: int
    strategy_id: str
    instrument: str
    account_id: int
    exchange: Exchange

    def __str__(self) -> str:
        return (
            f"{self.strategy_id}/{self.instrument}"
            f"@acct{self.account_id}#inst{self.instance_id}"
        )


@dataclass(frozen=True, slots=True)
class Tick:
    """One market-data update for a single instrument.

    Deliberately minimal for this first pass -- last-traded price plus its
    exchange timestamp is all the monitoring loop needs to recompute a
    combined premium. The concrete ``market_data`` package may deliver a
    richer object; strategies must depend only on the fields declared here.
    """

    instrument: InstrumentIdentifier
    last_price: Decimal
    timestamp: datetime


@runtime_checkable
class MarketDataGateway(Protocol):
    """Seam for live/streamed market data.

    The strategy monitor prefers pushed ticks (delivered via
    ``StrategyRunner.dispatch_tick``) but this gateway is the pull-based
    fallback the spec calls for when the websocket is degraded, plus the
    subscription control surface. To be implemented by ``market_data/`` later.
    """

    def subscribe(self, instruments: list[InstrumentIdentifier]) -> None:
        """Register interest so ticks for these instruments start flowing."""
        ...

    def unsubscribe(self, instruments: list[InstrumentIdentifier]) -> None:
        """Drop interest in these instruments."""
        ...

    def get_ltp(self, instrument: InstrumentIdentifier) -> Decimal:
        """Pull the current last-traded price for one instrument (fallback
        path used when streamed ticks are unavailable or stale)."""
        ...

    def get_ltps(
        self, instruments: list[InstrumentIdentifier]
    ) -> dict[InstrumentIdentifier, Decimal]:
        """Batched ``get_ltp`` -- e.g. both straddle legs in one call."""
        ...

    def is_connected(self) -> bool:
        """Whether the streaming feed is currently healthy (no I/O)."""
        ...


@dataclass(frozen=True, slots=True)
class RiskDecision:
    """Result of a pre-trade risk check: whether the action is approved, and
    if not, a human-readable reason for the audit trail / alert."""

    approved: bool
    reason: str | None = None


@runtime_checkable
class RiskGateway(Protocol):
    """Seam for the risk subsystem.

    Minimal but real: ``is_halted`` is the cheap, independent kill-switch /
    emergency-exit check, and ``approve_entry`` is the pre-trade gate an entry
    must pass before any order is placed (the concrete risk implementation,
    built in the risk task, fans this out to the daily-loss-limit,
    exposure-limit, and margin checks the spec names). The full surface --
    including exit-side checks that must never *block* a risk-reducing exit --
    belongs to the risk task and will extend the concrete implementation, not
    this Protocol.
    """

    def is_halted(self, identity: StrategyIdentity) -> bool:
        """True if a kill-switch / emergency-exit / freeze currently applies
        to this instance (globally, per-account, or per-instance). Cheap and
        safe to call frequently."""
        ...

    def approve_entry(self, identity: StrategyIdentity, *, quantity: int) -> RiskDecision:
        """Pre-trade gate for opening new exposure: approve or reject an entry
        of ``quantity`` per leg for this instance, having consulted whatever
        limits the risk subsystem enforces (daily loss, exposure, margin).
        Called *before* any order is placed; a rejection means no order is
        sent at all."""
        ...


@runtime_checkable
class TimeProvider(Protocol):
    """Seam for trading-calendar-aware time, to be implemented by
    ``services/time_service.py``.

    Injected rather than calling ``datetime.now()`` directly so tests can
    pin the clock and so IST/UTC handling lives in exactly one place -- the
    ``trade_date`` idempotency key depends on this being correct.
    """

    def now(self) -> datetime:
        """Current instant, timezone-aware, in UTC."""
        ...

    def now_ist(self) -> datetime:
        """Current instant, timezone-aware, in Asia/Kolkata (IST)."""
        ...

    def today(self) -> date:
        """Current IST calendar trading date -- the value written to
        ``Position.trade_date``."""
        ...


@runtime_checkable
class TradeExporter(Protocol):
    """Seam for the trade-report side channel, implemented by
    ``reporting/trade_report.py``.

    The engine calls this once, after a position reaches CLOSED, to render the
    day's completed trades to a file. It is deliberately fire-and-forget and
    must be exception-safe on its own -- the engine also guards the call, but
    an implementation must never raise a reporting failure back into the exit
    path. Optional (see ``StrategyContext``): when unset, no export happens.
    """

    def export_closed_position(self, position_id: int) -> None:
        """Render/refresh the report for the trading day the given (now CLOSED)
        position belongs to. Must not raise."""
        ...


@runtime_checkable
class TradeHistoryRecorder(Protocol):
    """Seam for permanent trade-history persistence, implemented by
    ``services/trade_history_recorder.py``.

    Called once, after a position reaches CLOSED, to write one append-only
    analytics row into ``trade_history``. Like ``TradeExporter`` it is
    fire-and-forget and must be exception-safe on its own -- a recording
    failure must never reach the exit path (the trade is safe in the execution
    tables and can be backfilled). Optional: when unset, no history is written.
    """

    def record_closed_position(self, position_id: int) -> None:
        """Persist the history row for the given (now CLOSED) position. Must not
        raise."""
        ...


@dataclass(frozen=True, slots=True)
class StrategyContext:
    """Everything a running strategy instance depends on, injected once at
    construction.

    Frozen: the wiring of an instance never changes over its life. Holds a
    ``session_factory`` (a ``sessionmaker``) rather than a live ``Session``
    on purpose -- a strategy's monitoring loop spans many ticks and must own
    its own transaction boundaries (open a short unit of work per state
    change), exactly as ``database/session.py`` documents for long-running
    callers. Handing it a single shared Session would be both unsafe (a
    Session is not thread-safe) and wrong (one endless transaction).

    The last five fields (``instrument_service``, ``expiry_service``,
    ``spot_price_provider``, ``trade_exporter``, ``trade_history_recorder``)
    are optional and default to ``None``: they are seams a *specific* strategy
    (e.g. Strategy-1, which needs strike/expiry resolution) may require, or
    purely observational side channels (``trade_exporter``,
    ``trade_history_recorder``), not something the generic engine itself
    depends on to trade. Keeping them optional here means every existing
    caller that
    builds a ``StrategyContext`` without them keeps working unchanged; a
    concrete strategy that needs one is responsible for checking it is
    present (and failing fast in its own constructor if not), the same way it
    already validates its own ``config`` type. This keeps
    ``StrategyContext`` itself strategy-agnostic while still letting it carry
    per-strategy extras.
    """

    identity: StrategyIdentity
    config: BaseModel
    session_factory: sessionmaker[Session]
    broker: BrokerBase
    market_data: MarketDataGateway
    risk: RiskGateway
    time: TimeProvider
    logger: logging.LoggerAdapter
    instrument_service: InstrumentService | None = None
    expiry_service: ExpiryService | None = None
    spot_price_provider: SpotPriceProvider | None = None
    trade_exporter: TradeExporter | None = None
    trade_history_recorder: TradeHistoryRecorder | None = None
