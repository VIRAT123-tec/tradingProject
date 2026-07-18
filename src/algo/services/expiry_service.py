"""Resolves the current weekly expiry date for an instrument (Nifty/NFO and
Sensex/BFO have different expiry weekdays -- do not assume shared logic),
including holiday-shifted expiries.

This module defines the *seam* -- the ``ExpiryService`` Protocol. The concrete
``services/live_seams.ConfigExpiryService`` computes the weekly expiry from a
configured weekday and holiday-shifts it via the trading calendar.

⚠️ That config-weekday implementation is a serviceable default, *not*
authoritative: exchanges have changed weekly-expiry weekdays for both Nifty and
Sensex more than once, and a holiday-shifted expiry does not always follow a
fixed formula. Its configured weekday must be validated against current
exchange rules before live use, and is ideally replaced by an
instrument-master / exchange-calendar source. strike_selector.py and other
callers depend only on this Protocol, so that replacement needs no caller
change.
"""

from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable


@runtime_checkable
class ExpiryService(Protocol):
    """Seam for weekly-expiry resolution."""

    def get_current_weekly_expiry(self, instrument: str, as_of: date) -> date:
        """Return the current weekly expiry date for ``instrument`` as of the
        trading date ``as_of``.

        "Current" means the nearest weekly expiry on or after ``as_of`` -- if
        ``as_of`` itself is an expiry day, the convention is that the current
        expiry *is* today (a same-day, "0DTE" expiry), not next week's. Whether
        the strategy chooses to trade a 0DTE expiry is an entry-logic policy
        decision made elsewhere; this method only answers "what date is the
        current weekly expiry", never "should we trade it".

        Implementations must account for holiday-shifted expiries (an expiry
        falling on an exchange holiday moves according to exchange rules, not a
        fixed offset) and must raise rather than guess if the answer cannot be
        determined from the underlying data (e.g. instrument master not yet
        synced far enough into the future).
        """
        ...
