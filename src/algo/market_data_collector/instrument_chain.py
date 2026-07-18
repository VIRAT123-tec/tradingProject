"""KiteMarketReader: the collector's read-only Kite REST access for spot LTP and
option-chain enumeration.

Reuses the same construction pattern as ``services/live_seams.build_kite_tick_stream``
(a lazily-built ``KiteConnect`` from env) and the same instrument-dump approach
as ``KiteInstrumentTokenMap`` -- but exposes what the collector needs: the spot
LTP for an underlying (to compute ATM) and the full option chain
(strike -> CE/PE tradingsymbol + instrument_token) for a given expiry, resolved
from the authoritative Kite dump. Instrument metadata (exchange, spot symbol,
and the dump underlying name, e.g. SENSEXBANK -> BANKEX) comes from the reused
``ConfigInstrumentService``.
"""

from __future__ import annotations

import threading
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Callable

from algo.brokers.kite import mapper as kite_mapper

if TYPE_CHECKING:
    from algo.services.live_seams import ConfigInstrumentService

# strike -> {"CE": (tradingsymbol, token), "PE": (tradingsymbol, token)}
OptionChain = dict[Decimal, dict[str, tuple[str, int]]]


class KiteMarketReader:
    """Read-only Kite REST helper: spot LTP + option chain from the dump."""

    def __init__(
        self,
        *,
        client_factory: Callable[[], Any],
        instrument_service: ConfigInstrumentService,
    ) -> None:
        self._client_factory = client_factory
        self._instruments = instrument_service
        self._client: Any | None = None
        self._dumps: dict[str, list[dict[str, Any]]] = {}
        self._lock = threading.Lock()

    def _get_client(self) -> Any:
        with self._lock:
            if self._client is None:
                self._client = self._client_factory()
            return self._client

    def _dump(self, kite_exchange: str) -> list[dict[str, Any]]:
        with self._lock:
            cached = self._dumps.get(kite_exchange)
            if cached is not None:
                return cached
            if self._client is None:
                self._client = self._client_factory()
            rows = self._client.instruments(kite_exchange)
            self._dumps[kite_exchange] = rows
            return rows

    def spot_ltp(self, underlying: str) -> Decimal:
        """Current spot LTP for the underlying index (cash segment)."""
        exchange, symbol = self._instruments.spot_reference(underlying)
        key = f"{exchange.value}:{symbol}"
        data = self._get_client().ltp([key])
        entry = data.get(key)
        if not entry or entry.get("last_price") is None:
            raise LookupError(f"no spot LTP for {underlying} ({key})")
        return Decimal(str(entry["last_price"]))

    def option_chain(self, underlying: str, expiry: date) -> OptionChain:
        """The full CE/PE chain (strike -> symbols+tokens) for an underlying at a
        given expiry, from the Kite instrument dump."""
        spec = self._instruments.get_instrument_spec(underlying)
        dump_name = spec.underlying_symbol or underlying
        kite_exchange = kite_mapper.to_kite_exchange(spec.exchange)
        chain: OptionChain = {}
        for row in self._dump(kite_exchange):
            if (
                row.get("name") == dump_name
                and row.get("instrument_type") in ("CE", "PE")
                and row.get("expiry") == expiry
            ):
                token = row.get("instrument_token")
                symbol = row.get("tradingsymbol")
                strike = row.get("strike")
                if token is None or not symbol or strike is None:
                    continue
                chain.setdefault(Decimal(str(strike)), {})[row["instrument_type"]] = (
                    symbol, int(token)
                )
        return chain
