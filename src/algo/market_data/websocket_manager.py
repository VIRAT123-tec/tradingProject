"""The broker-agnostic websocket seam for live market-data ticks.

This module defines the *contract* the market-data service depends on -- it does
not contain any broker (Kite) code. The concrete Kite implementation (a market-
data KiteTicker wrapped with instrument-token mapping and reconnect-with-backoff)
lives in the ``brokers/kite`` package, satisfying this Protocol; injecting it is
what keeps all broker-specific logic isolated there and lets the service be
tested against a fake tick stream with no network.

Two seams live here:

* ``TickStream`` -- the streaming websocket: start/stop, subscribe/unsubscribe by
  broker-agnostic ``InstrumentIdentifier`` (the adapter maps to tokens), a
  connection-status check, and the four handler hooks the service wires
  (on_tick / on_connect / on_disconnect / on_reconnect). Reconnect-with-backoff
  is the implementation's responsibility (KiteTicker does it natively); the
  service reacts to it via ``on_reconnect`` by re-subscribing.
* ``LtpPoller`` -- the pull-based fallback source (a batched LTP read). The
  already-built ``BrokerBase`` satisfies this structurally via ``get_ltp``
  (already batched: ``list[InstrumentIdentifier] -> dict[...]``, despite the
  singular name), so the broker itself is injected as the poller; the narrow
  Protocol keeps the market-data layer from depending on the whole broker
  surface.
"""

from __future__ import annotations

from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Callable

    from algo.brokers.broker_base import InstrumentIdentifier
    from algo.strategy_engine.strategy_context import Tick


class ConnectionState(str, Enum):
    """Coarse connection state emitted for monitoring/heartbeat."""

    DISCONNECTED = "DISCONNECTED"
    CONNECTED = "CONNECTED"


@runtime_checkable
class TickStream(Protocol):
    """Broker-agnostic live-tick websocket. Implemented by a broker adapter
    (e.g. a Kite market-data ticker); tested against a fake."""

    def set_handlers(
        self,
        *,
        on_tick: Callable[[Tick], None],
        on_connect: Callable[[], None],
        on_disconnect: Callable[[], None],
        on_reconnect: Callable[[], None],
    ) -> None:
        """Register the four lifecycle handlers. Called once by the service
        before ``start``. ``on_reconnect`` fires after the stream has
        re-established a dropped connection (distinct from the first
        ``on_connect``), which is the service's cue to re-subscribe."""
        ...

    def start(self) -> None:
        """Open the connection (on its own background thread) and begin
        streaming. Idempotent: a no-op if already started."""
        ...

    def stop(self) -> None:
        """Close the connection and stop reconnecting. Idempotent."""
        ...

    def is_connected(self) -> bool:
        """Whether the socket is currently connected. No I/O."""
        ...

    def subscribe(self, instruments: list[InstrumentIdentifier]) -> None:
        """Start streaming ticks for these instruments."""
        ...

    def unsubscribe(self, instruments: list[InstrumentIdentifier]) -> None:
        """Stop streaming ticks for these instruments."""
        ...


@runtime_checkable
class LtpPoller(Protocol):
    """Broker-agnostic pull source for last-traded prices -- the polling
    fallback. ``BrokerBase`` satisfies this structurally via its own
    ``get_ltp`` (named to match ``BrokerBase.get_ltp`` exactly, not
    ``get_ltps`` -- a real, previously-uncaught naming drift between this
    module and ``brokers/broker_base.py`` that surfaced only once an
    integration test drove a real broker through this polling path; see
    Task 26's own summary)."""

    def get_ltp(
        self, instruments: list[InstrumentIdentifier], *, timeout: float | None = ...
    ) -> dict[InstrumentIdentifier, Decimal]:
        """Fetch last-traded prices for a batch of instruments in as few broker
        calls as possible."""
        ...
