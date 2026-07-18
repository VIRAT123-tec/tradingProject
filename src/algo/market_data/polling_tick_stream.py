"""PollingTickStream: a TickStream that never opens a websocket.

It satisfies the ``TickStream`` seam but deliberately pushes no live ticks and
reports ``is_connected() == False`` always. That is not a degraded mode -- it
is a correct, safe default: with no live feed, ``MarketDataService`` serves
every price read from its polling fallback (a broker ``get_ltp`` call), and the
monitoring heartbeat evaluates every open position on its cadence regardless.
The platform is therefore fully functional (entries, stop-loss/target exits,
kill-switch) on polling alone, with no dependency on a live market websocket.

This is the default stream the live/paper startup wires in. A low-latency
push feed (e.g. the Kite market ticker in ``brokers/kite/market_ticker.py``)
can be swapped in later without any change to the rest of the platform, because
everything depends only on the ``TickStream`` seam.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from algo.brokers.broker_base import InstrumentIdentifier
    from algo.strategy_engine.strategy_context import Tick


class PollingTickStream:
    """A no-websocket ``TickStream``. Reports itself disconnected so all reads
    go through the market-data polling fallback."""

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self._logger = logger if logger is not None else logging.getLogger("algo.market_data.polling_stream")

    def set_handlers(
        self,
        *,
        on_tick: Callable[[Tick], None],
        on_connect: Callable[[], None],
        on_disconnect: Callable[[], None],
        on_reconnect: Callable[[], None],
    ) -> None:
        # No live feed: the handlers are never invoked. Accepted for Protocol
        # conformance so the service can wire itself uniformly.
        pass

    def start(self) -> None:
        self._logger.info(
            "market data running in polling-only mode (no live tick websocket); "
            "prices are served by broker polling on demand"
        )

    def stop(self) -> None:
        pass

    def is_connected(self) -> bool:
        return False

    def subscribe(self, instruments: list[InstrumentIdentifier]) -> None:
        pass

    def unsubscribe(self, instruments: list[InstrumentIdentifier]) -> None:
        pass
