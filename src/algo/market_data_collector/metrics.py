"""CollectorMetrics: runtime counters + the periodic "Collector Status" render.

Holds the two hot-path counters the collector increments (ticks received,
reconnects) and reads the rest of the picture from the live components (the
tick stream, the writer, the ATM manager). ``render()`` produces the operator-
facing status block; the orchestrator logs it every ``metrics.interval_seconds``.
Counters are plain ints (approximate by design -- an occasional lost increment
on the socket hot path is fine for a metric and avoids lock contention).
"""

from __future__ import annotations

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

    def on_tick(self) -> None:
        self.ticks_received += 1

    def on_reconnect(self) -> None:
        self.reconnect_count += 1

    def render(self) -> str:
        lines = [
            "Collector Status",
            f"  Connected              : {'YES' if self._stream.is_connected() else 'NO'}",
            f"  Subscribed Instruments : {self._stream.subscribed_count}",
            f"  Ticks Received         : {self.ticks_received:,}",
            f"  Rows Written           : {self._writer.rows_written:,}",
            f"  Queue Size             : {self._writer.queue_size:,}",
            f"  Batch Time             : {self._writer.last_batch_seconds * 1000:.1f} ms",
            f"  Reconnect Count        : {self.reconnect_count}",
            f"  Dropped Ticks          : {self._writer.dropped_ticks:,}",
            "Current ATM:",
        ]
        for u in self._underlyings:
            atm = self._atm.current_atm(u)
            lines.append(f"  {u:<8} : {atm if atm is not None else '—'}")
        return "\n".join(lines)
