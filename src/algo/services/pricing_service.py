"""Shared pricing lookups (LTP, quote) that don't belong to a specific market_data
manager, used where a simple one-off price read is needed.

Defines the ``SpotPriceProvider`` seam -- how entry logic reads the *underlying
index spot* LTP (e.g. NIFTY 50, SENSEX) to compute the ATM strike. The concrete
``services/live_seams.BrokerSpotPriceProvider`` reads it through the broker's
cash-segment ``get_ltp`` (the ``Exchange`` enum now includes the NSE/BSE cash
segments the index spot lives on, which are used only for these reads and never
persisted). Callers depend only on this Protocol.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Protocol, runtime_checkable


@runtime_checkable
class SpotPriceProvider(Protocol):
    """Seam for reading an underlying instrument's spot LTP by its logical
    name (e.g. "NIFTY", "SENSEX")."""

    def get_spot_ltp(self, instrument: str) -> Decimal:
        """Return the current spot LTP for ``instrument``'s underlying index.

        Implementations must raise rather than return a guessed or stale price
        if a live value is unavailable -- a wrong spot LTP selects the wrong
        ATM strike, so a silent fallback here would place real orders on the
        wrong contracts.
        """
        ...
