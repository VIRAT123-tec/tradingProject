"""Single source of truth for "current IST time" across the platform.

Unlike ``instrument_service.py``/``expiry_service.py``/``pricing_service.py``
(which define seams only, since a real implementation needs data that does not
exist yet), this module provides a real, complete implementation:
``SystemTimeProvider`` needs no external data, only a correct timezone
conversion, so there is no honest reason to leave it a stub.

Every other module that needs "now" (strategies, risk, reconciliation, the
scheduler, market data) receives it through the injected
``strategy_context.TimeProvider`` Protocol, never by calling
``datetime.now()`` itself -- this is the one place in the whole platform where
that call is allowed to happen, which is what keeps every consumer mockable
(a test injects a fixed/manually-advanced clock instead) and guards against
the naive/aware datetime mixing bugs that would otherwise fire entry/exit
logic at the wrong wall-clock time.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytz

_IST = pytz.timezone("Asia/Kolkata")


class SystemTimeProvider:
    """Wall-clock ``TimeProvider`` implementation."""

    def now(self) -> datetime:
        """Current instant, timezone-aware, in UTC."""
        return datetime.now(timezone.utc)

    def now_ist(self) -> datetime:
        """Current instant, timezone-aware, in Asia/Kolkata (IST)."""
        return datetime.now(_IST)

    def today(self) -> date:
        """Current IST calendar date -- the value written to
        ``Position.trade_date``."""
        return self.now_ist().date()
