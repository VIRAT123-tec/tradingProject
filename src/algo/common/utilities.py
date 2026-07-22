"""Small, dependency-free utility functions with no natural home elsewhere.

TODO: keep this file deliberately thin — anything domain-specific belongs in
      services/ instead of accumulating here.
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal

_CENTS = Decimal("0.01")


def pnl_per_share(total_pnl: Decimal | None, lot_size: int | None) -> Decimal | None:
    """Per-share (per-unit) P&L = total P&L / lot size, rounded to two decimals.

    Additive analytics only: it never affects the stored total P&L, which
    remains the single source of truth. The single definition of this formula --
    reused by the Excel report, the trade_history row, and the position-close
    summary log -- so all three agree exactly.

    Returns ``None`` (rendered as blank / stored as NULL) when it cannot be
    computed: a missing total P&L, or a missing/zero lot size (never divides by
    zero). ``0`` total P&L with a valid lot size yields ``Decimal('0.00')``.
    """
    if total_pnl is None or not lot_size:
        return None
    return (Decimal(total_pnl) / Decimal(lot_size)).quantize(_CENTS, rounding=ROUND_HALF_UP)
