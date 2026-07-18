"""TickWriter: the non-blocking persistence path.

Hard rule: the websocket thread must never touch the database. It calls
``enqueue()`` -- an O(1), lock-guarded append to a bounded in-memory deque that
never blocks. A dedicated writer thread drains the deque and bulk-inserts into
the market-data database:

    websocket thread --enqueue--> bounded deque --writer thread--> COPY --> DB

Writes are flushed on ``batch_size`` rows or every ``flush_interval_ms``,
whichever first, via Postgres ``COPY`` (the fastest ingest path) with a portable
fallback to a batched ``executemany`` INSERT (used on SQLite in tests, or when
``use_copy`` is false). If the database is unreachable the writer retries with
capped exponential backoff while the deque keeps buffering; on overflow it sheds
per ``overflow_policy`` and counts the drop. Nothing here can stall the socket
or crash the process.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import threading
import time
from collections import deque
from decimal import Decimal
from typing import TYPE_CHECKING

from algo.market_data_collector.db import TICK_COLUMNS, MarketTick, option_ticks

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from algo.market_data_collector.config import WriterConfig

_logger = logging.getLogger("algo.collector.writer")

_DEPTH_IDX = TICK_COLUMNS.index("depth")


class TickWriter:
    """Bounded queue + background batched writer to the market-data DB."""

    def __init__(self, *, engine: Engine, config: WriterConfig, logger: logging.Logger | None = None) -> None:
        self._engine = engine
        self._cfg = config
        self._logger = logger or _logger
        self._is_postgres = engine.dialect.name == "postgresql"

        self._dq: deque[MarketTick] = deque()
        self._lock = threading.Lock()
        self._flush = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

        # Metrics (read by metrics.py; simple ints/floats are atomic enough here).
        self.rows_written = 0
        self.dropped_ticks = 0
        self.db_errors = 0
        self.last_batch_seconds = 0.0

    # -- Lifecycle -----------------------------------------------------

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="collector-writer", daemon=True)
        self._thread.start()

    def stop(self, *, drain: bool = True) -> None:
        """Stop the writer. If ``drain``, flush whatever is queued first."""
        if drain:
            self._flush.set()
        self._stop.set()
        self._flush.set()
        t = self._thread
        self._thread = None
        if t is not None:
            t.join(timeout=30)

    @property
    def queue_size(self) -> int:
        with self._lock:
            return len(self._dq)

    # -- Producer side (socket thread) ---------------------------------

    def enqueue(self, tick: MarketTick) -> None:
        """Append a tick. Never blocks; applies the overflow policy when the
        bounded deque is full."""
        with self._lock:
            if len(self._dq) >= self._cfg.queue_max:
                if self._cfg.overflow_policy == "drop_oldest":
                    self._dq.popleft()
                    self.dropped_ticks += 1
                else:  # drop_newest
                    self.dropped_ticks += 1
                    return
            self._dq.append(tick)
            full_batch = len(self._dq) >= self._cfg.batch_size
        if full_batch:
            self._flush.set()

    # -- Consumer side (writer thread) ---------------------------------

    def _run(self) -> None:
        interval = self._cfg.flush_interval_ms / 1000.0
        while not self._stop.is_set():
            self._flush.wait(timeout=interval)
            self._flush.clear()
            self._drain_and_write()
        self._drain_and_write()  # final flush on shutdown

    def _drain_and_write(self) -> None:
        while True:
            batch = self._take_batch()
            if not batch:
                return
            self._write_with_retry(batch)

    def _take_batch(self) -> list[MarketTick]:
        with self._lock:
            n = min(self._cfg.batch_size, len(self._dq))
            return [self._dq.popleft() for _ in range(n)]

    def _write_with_retry(self, batch: list[MarketTick]) -> None:
        delay = self._cfg.db_retry_base_seconds
        attempt = 0
        while True:
            try:
                started = time.monotonic()
                self._write_batch(batch)
                self.last_batch_seconds = time.monotonic() - started
                self.rows_written += len(batch)
                return
            except Exception:  # noqa: BLE001 -- DB hiccup: retry, never crash the collector
                self.db_errors += 1
                attempt += 1
                self._logger.error(
                    "market-data write failed (attempt %d, %d rows); retrying in %.1fs",
                    attempt, len(batch), delay, exc_info=True,
                )
                # While shutting down, don't retry forever -- log the loss and move on.
                if self._stop.is_set() and attempt >= 3:
                    self._logger.critical(
                        "dropping %d market-data rows: DB unreachable during shutdown", len(batch)
                    )
                    self.dropped_ticks += len(batch)
                    return
                time.sleep(delay)
                delay = min(delay * 2, self._cfg.db_retry_max_seconds)

    def _write_batch(self, batch: list[MarketTick]) -> None:
        if self._cfg.use_copy and self._is_postgres:
            self._copy_batch(batch)
        else:
            self._insert_batch(batch)

    def _insert_batch(self, batch: list[MarketTick]) -> None:
        rows = [dict(zip(TICK_COLUMNS, t.as_row())) for t in batch]
        with self._engine.begin() as conn:
            conn.execute(option_ticks.insert(), rows)

    def _copy_batch(self, batch: list[MarketTick]) -> None:
        buf = io.StringIO()
        writer = csv.writer(buf)
        for t in batch:
            row = list(t.as_row())
            depth = row[_DEPTH_IDX]
            row[_DEPTH_IDX] = json.dumps(depth) if depth is not None else None
            writer.writerow(["" if v is None else (str(v) if isinstance(v, Decimal) else v) for v in row])
        buf.seek(0)
        cols = ", ".join(TICK_COLUMNS)
        raw = self._engine.raw_connection()
        try:
            with raw.cursor() as cur:
                cur.copy_expert(f"COPY option_ticks ({cols}) FROM STDIN WITH (FORMAT csv, NULL '')", buf)
            raw.commit()
        finally:
            raw.close()
