"""Resolves instrument metadata (lot size, exchange segment, strike interval,
tick size) from configs/instruments/*.yaml and the synced instrument master.

This module defines the *seam* -- the ``InstrumentService`` Protocol and the
``InstrumentSpec`` value it returns. The concrete, config-reading
implementation is ``services/live_seams.ConfigInstrumentService``, which reads
``configs/instruments/*.yaml`` (the strike interval, lot size, and expiry
weekday there are flagged as values that change and must be confirmed before
live use, never hardcoded). Consumers (strike_selector.py, entry_logic.py)
depend only on this Protocol, so the concrete implementation -- or a future
instrument-master-backed one -- can be swapped in without changing any caller.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol, runtime_checkable

from algo.common.enums import Exchange


@dataclass(frozen=True, slots=True)
class InstrumentSpec:
    """Static, config-driven metadata for one instrument (e.g. NIFTY, SENSEX).

    Distinct from ``algo.brokers.broker_base.BrokerInstrument``, which describes
    one specific broker-side contract (a particular strike/expiry/option_type).
    ``InstrumentSpec`` describes the instrument itself, independent of any
    single option contract -- the values strike_selector.py needs *before* it
    can ask the broker for a specific contract.
    """

    instrument: str
    exchange: Exchange
    strike_interval: Decimal
    lot_size: int
    tick_size: Decimal
    underlying_symbol: str | None = None
    """The name this instrument's option contracts carry in the broker's
    instrument dump, when it differs from ``instrument`` (the platform display
    identity). ``None`` means they are the same -- the common case (NIFTY,
    SENSEX, BANKNIFTY, ...). Only differs for an index whose display name is
    not its contract name, e.g. identity ``SENSEXBANK`` -> contract name
    ``BANKEX``. Used solely for contract lookup; the identity is what appears in
    the database, logs, and reports."""

    def __post_init__(self) -> None:
        if not self.instrument:
            raise ValueError("InstrumentSpec.instrument must be a non-empty string")
        if self.strike_interval <= 0:
            raise ValueError(
                f"InstrumentSpec.strike_interval must be positive, got {self.strike_interval}"
            )
        if self.lot_size <= 0:
            raise ValueError(f"InstrumentSpec.lot_size must be positive, got {self.lot_size}")
        if self.tick_size <= 0:
            raise ValueError(f"InstrumentSpec.tick_size must be positive, got {self.tick_size}")


@runtime_checkable
class InstrumentService(Protocol):
    """Seam for instrument metadata lookup.

    A future concrete implementation reads ``configs/instruments/*.yaml``
    (validated via Pydantic, per the platform's config-loading convention) and/or
    the synced instrument master. Nothing in this Protocol assumes where the
    data comes from -- callers only need ``get_instrument_spec``.
    """

    def get_instrument_spec(self, instrument: str) -> InstrumentSpec:
        """Return the static spec for ``instrument`` (e.g. "NIFTY", "SENSEX").

        Implementations should raise a ``KeyError`` (or a documented subclass)
        for an unknown instrument name -- never fall back to a guessed default,
        since every value here (strike interval, lot size) directly determines
        order quantity and strike selection.
        """
        ...
