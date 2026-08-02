"""Strategy-1 execution exceptions.

Dedicated exception types for genuinely-unexpected broker/execution conditions
that must fail loudly rather than be papered over with a default value.
"""

from __future__ import annotations


class MissingFillPriceError(Exception):
    """A broker order reached a COMPLETE/terminal state but carried no usable
    fill price (``average_price`` is ``None`` or non-positive).

    This is a money-path integrity failure: substituting 0 would silently
    corrupt the entry premium, target/stoploss levels, realized P&L, and trade
    history. Raising instead lets the platform's existing freeze + recovery +
    reconciliation machinery resolve the position against broker truth, rather
    than persisting a wrong number. A real option execution price is always
    strictly positive, so this never fires for a genuine fill.
    """
