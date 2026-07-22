"""CollectorMetrics: runtime counters + the periodic "Collector Status" render.

Holds the two hot-path counters the collector increments (ticks received,
reconnects) and reads the rest of the picture from the live components (the
tick stream, the writer, the ATM manager). ``render()`` produces the operator-
facing status block; the orchestrator logs it every ``metrics.interval_seconds``.
Counters are plain ints (approximate by design -- an occasional lost increment
on the socket hot path is fine for a metric and avoids lock contention).
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from algo.market_data_collector.atm_window import AtmWindowManager
    from algo.market_data_collector.full_tick_stream import CollectorTickStream
    from algo.market_data_collector.tick_writer import TickWriter


class CollectorMetrics:
    def __init__(
        self,
        *,
        stream: CollectorTickStream,
        writer: TickWriter,
        atm: AtmWindowManager,
        underlyings: list[str],
    ) -> None:
        self._stream = stream
        self._writer = writer
        self._atm = atm
        self._underlyings = underlyings
        self.ticks_received = 0
        self.reconnect_count = 0
        self.restart_count = 0
        self.last_tick_at = 0.0  # monotonic() of the most recent tick; 0.0 = none yet

    def on_tick(self) -> None:
        self.ticks_received += 1
        self.last_tick_at = time.monotonic()

    def on_reconnect(self) -> None:
        self.reconnect_count += 1

    def on_restart(self) -> None:
        self.restart_count += 1

    def render(self) -> str:
        connected = self._stream.is_connected()
        # Task 3: a disconnected socket is NOT subscribed to anything live, even
        # though the intended-window cache still holds the tokens for the rebuild.
        subscribed = f"{self._stream.subscribed_count}" if connected else "0 (Disconnected)"
        if self.last_tick_at > 0.0:
            since_tick = f"{time.monotonic() - self.last_tick_at:.0f} s"
        else:
            since_tick = "—"
        lines = [
            "Collector Status",
            f"  Connected              : {'YES' if connected else 'NO'}",
            f"  Subscribed Instruments : {subscribed}",
            f"  Ticks Received         : {self.ticks_received:,}",
            f"  Since Last Tick        : {since_tick}",
            f"  Rows Written           : {self._writer.rows_written:,}",
            f"  Queue Size             : {self._writer.queue_size:,}",
            f"  Batch Time             : {self._writer.last_batch_seconds * 1000:.1f} ms",
            f"  Reconnect Count        : {self.reconnect_count}",
            f"  Restart Count          : {self.restart_count}",
            f"  Dropped Ticks          : {self._writer.dropped_ticks:,}",
            "Current ATM:",
        ]
        for u in self._underlyings:
            atm = self._atm.current_atm(u)
            lines.append(f"  {u:<8} : {atm if atm is not None else '—'}")
        return "\n".join(lines)
