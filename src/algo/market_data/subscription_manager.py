"""Reference-counted subscription bookkeeping.

Tracks which instruments are currently subscribed, and by how many independent
consumers. Reference counting matters because two strategy instances can
legitimately want the same instrument at once: when one unsubscribes, the
websocket subscription must survive as long as the other still needs it. Only a
0->1 transition warrants an actual websocket ``subscribe`` call, and only a
1->0 transition warrants an ``unsubscribe`` -- this class computes exactly those
deltas so the service issues the minimal set of broker calls.

It also holds the authoritative "everything currently needed" set, which is
what gets re-subscribed wholesale after a websocket reconnect (a reconnected
socket has no memory of prior subscriptions).

Thread-safe: subscription changes and the reconnect resubscribe can arrive on
different threads.
"""

from __future__ import annotations

import threading
from collections.abc import Iterable

from algo.brokers.broker_base import InstrumentIdentifier


class SubscriptionManager:
    """Reference-counted set of subscribed instruments."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts: dict[InstrumentIdentifier, int] = {}

    def subscribe(self, instruments: Iterable[InstrumentIdentifier]) -> list[InstrumentIdentifier]:
        """Increment the reference count for each instrument. Returns the subset
        that transitioned 0->1 -- i.e. the instruments that now require an actual
        websocket subscription (duplicates within the call are counted once)."""
        newly_active: list[InstrumentIdentifier] = []
        with self._lock:
            for instrument in instruments:
                previous = self._counts.get(instrument, 0)
                self._counts[instrument] = previous + 1
                if previous == 0 and instrument not in newly_active:
                    newly_active.append(instrument)
        return newly_active

    def unsubscribe(self, instruments: Iterable[InstrumentIdentifier]) -> list[InstrumentIdentifier]:
        """Decrement the reference count for each instrument. Returns the subset
        that transitioned 1->0 -- i.e. the instruments that no longer have any
        consumer and can be unsubscribed at the websocket. Decrementing an
        instrument that is not subscribed is a harmless no-op."""
        newly_inactive: list[InstrumentIdentifier] = []
        with self._lock:
            for instrument in instruments:
                previous = self._counts.get(instrument, 0)
                if previous <= 0:
                    continue
                if previous == 1:
                    del self._counts[instrument]
                    if instrument not in newly_inactive:
                        newly_inactive.append(instrument)
                else:
                    self._counts[instrument] = previous - 1
        return newly_inactive

    def active(self) -> list[InstrumentIdentifier]:
        """Every instrument currently subscribed by at least one consumer -- the
        set to re-subscribe wholesale after a reconnect."""
        with self._lock:
            return list(self._counts.keys())

    def is_subscribed(self, instrument: InstrumentIdentifier) -> bool:
        with self._lock:
            return self._counts.get(instrument, 0) > 0

    def reference_count(self, instrument: InstrumentIdentifier) -> int:
        with self._lock:
            return self._counts.get(instrument, 0)
