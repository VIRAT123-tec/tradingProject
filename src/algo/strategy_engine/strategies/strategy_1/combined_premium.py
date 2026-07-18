"""Computes combined straddle premium (CE LTP + PE LTP) and the derived
target/stoploss premiums from strategy config percentages, and tracks the live
combined premium as market ticks arrive.

Scope boundary: this module computes and tracks numbers -- it never decides
anything. It does not know what "target hit" or "stoploss hit" means as an
action, does not place orders, does not read or write the database, and does
not touch the broker. exit_logic.py (a separate, later module) is the one that
reads the values this module produces and applies the exit priority order
(cutoff -> stoploss -> target -> kill-switch) the functional spec defines.
Keeping that decision entirely out of here is deliberate: a straddle-premium
computation with an "is_target_hit()" method sitting inside it is exactly the
kind of quiet scope creep that turns a small, easily-verified module into one
that is hard to reason about.

Money-critical arithmetic: every computation here is a pure, module-level
function with no I/O, so it is exhaustively unit-testable independent of the
stateful ``CombinedPremiumTracker`` that wraps them for live tick handling.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from algo.brokers.broker_base import InstrumentIdentifier
    from algo.strategy_engine.strategy_context import StrategyContext, Tick


# ---------------------------------------------------------------------------
# Pure computation
# ---------------------------------------------------------------------------


def compute_combined_premium(ce_price: Decimal, pe_price: Decimal) -> Decimal:
    """Combined straddle premium = CE_LTP + PE_LTP.

    Both prices must be non-negative -- an option premium can legitimately be
    zero (deep out-of-the-money, close to expiry) but never negative; a
    negative input indicates corrupt market data upstream and must not be
    silently summed into a wrong (but plausible-looking) combined premium.
    """
    if ce_price < 0:
        raise ValueError(f"ce_price must be non-negative, got {ce_price}")
    if pe_price < 0:
        raise ValueError(f"pe_price must be non-negative, got {pe_price}")
    return ce_price + pe_price


def compute_target_premium(entry_premium: Decimal, target_pct: Decimal) -> Decimal:
    """target_premium = entry_premium * (1 - target_pct).

    This is a *short* straddle: premium is collected at entry, so profit is
    realized as the combined premium decays. The target is therefore below the
    entry premium, and ``target_pct`` is validated to be strictly between 0 and
    1 (exclusive) -- 0 would mean "target equals entry" (meaningless) and 1 or
    above would mean "target is zero or negative" (unreachable/nonsensical),
    matching the Position table's own ``target_pct_in_range`` CHECK constraint
    exactly, so a config value that would violate the database fails here
    first, at config-load time.
    """
    _validate_fraction(target_pct, "target_pct")
    _validate_positive_premium(entry_premium, "entry_premium")
    return entry_premium * (Decimal("1") - target_pct)


def compute_stoploss_premium(entry_premium: Decimal, sl_pct: Decimal) -> Decimal:
    """stoploss_premium = entry_premium * (1 + sl_pct).

    Mirror of ``compute_target_premium``: loss on a short straddle accrues as
    the combined premium *rises*, so the stoploss level is above the entry
    premium. Same (0, 1) exclusive validation as ``target_pct``, matching the
    Position table's ``sl_pct_in_range`` CHECK constraint.
    """
    _validate_fraction(sl_pct, "sl_pct")
    _validate_positive_premium(entry_premium, "entry_premium")
    return entry_premium * (Decimal("1") + sl_pct)


@dataclass(frozen=True, slots=True)
class PremiumThresholds:
    """The entry premium and the two levels derived from it, computed once
    and immutable for the life of a position -- exactly the values the
    Position row's own ``combined_entry_premium``/``target_premium``/
    ``stoploss_premium`` columns persist.
    """

    entry_premium: Decimal
    target_premium: Decimal
    stoploss_premium: Decimal
    target_pct: Decimal
    sl_pct: Decimal


def compute_thresholds(
    entry_premium: Decimal, target_pct: Decimal, sl_pct: Decimal
) -> PremiumThresholds:
    """Compute both derived levels from one entry premium in one call --
    the usual entry-time computation, bundled so callers (and the persistence
    code that writes the Position row) get a single consistent object rather
    than two separate calls that could theoretically be given inconsistent
    inputs.
    """
    return PremiumThresholds(
        entry_premium=entry_premium,
        target_premium=compute_target_premium(entry_premium, target_pct),
        stoploss_premium=compute_stoploss_premium(entry_premium, sl_pct),
        target_pct=target_pct,
        sl_pct=sl_pct,
    )


def _validate_fraction(value: Decimal, field_name: str) -> None:
    if not (Decimal("0") < value < Decimal("1")):
        raise ValueError(
            f"{field_name} must be strictly between 0 and 1 (exclusive), got {value}"
        )


def _validate_positive_premium(value: Decimal, field_name: str) -> None:
    if value <= 0:
        raise ValueError(f"{field_name} must be positive, got {value}")


# ---------------------------------------------------------------------------
# Live tracking
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PremiumSnapshot:
    """A point-in-time combined-premium reading, with the two leg prices it
    was computed from -- kept alongside the combined value so a log line or
    an exit-reason message can show "142.50 (CE 71.20 + PE 71.30)" without a
    second lookup.
    """

    combined_premium: Decimal
    ce_price: Decimal
    pe_price: Decimal
    as_of: datetime


class CombinedPremiumTracker:
    """Tracks live CE/PE prices for one straddle position and recomputes the
    combined premium as ticks arrive.

    Deliberately takes ``target_pct``/``sl_pct`` as plain ``Decimal`` values in
    its primary constructor, not a ``StrategyContext`` -- this is what keeps
    the class constructible with nothing but numbers in a unit test. Real
    callers (entry_logic.py, strategy.py) are expected to read those two
    values from the strategy's own validated config, reached through
    ``StrategyContext.config``; the ``from_context`` classmethod below does
    exactly that for convenience, so "read configuration from StrategyContext"
    is satisfied without forcing every test to construct a full context just
    to exercise premium arithmetic.

    Holds no trading logic: it answers "what is the combined premium right
    now" and "what are the target/stoploss levels", never "should we exit".
    """

    def __init__(
        self,
        *,
        call_instrument: InstrumentIdentifier,
        put_instrument: InstrumentIdentifier,
        target_pct: Decimal,
        sl_pct: Decimal,
    ) -> None:
        if call_instrument == put_instrument:
            raise ValueError(
                "call_instrument and put_instrument must be different "
                f"instruments, both got {call_instrument}"
            )
        _validate_fraction(target_pct, "target_pct")
        _validate_fraction(sl_pct, "sl_pct")

        self._call_instrument = call_instrument
        self._put_instrument = put_instrument
        self._target_pct = target_pct
        self._sl_pct = sl_pct

        self._latest_call_price: Decimal | None = None
        self._latest_put_price: Decimal | None = None
        self._latest_price_at: datetime | None = None
        self._thresholds: PremiumThresholds | None = None

    @classmethod
    def from_context(
        cls,
        context: StrategyContext,
        *,
        call_instrument: InstrumentIdentifier,
        put_instrument: InstrumentIdentifier,
    ) -> "CombinedPremiumTracker":
        """Build a tracker reading ``target_pct``/``sl_pct`` off
        ``context.config``.

        Uses attribute access rather than an ``isinstance`` check against
        Strategy-1's own config class deliberately: any config type exposing
        ``target_pct``/``sl_pct`` as ``Decimal`` works, matching the
        Protocol-based seams used elsewhere in the strategy engine rather than
        hard-coupling this module to one concrete schema class. Raises
        ``TypeError`` if the injected config does not expose both fields --
        loud and immediate, not a silent fallback to a guessed percentage.
        """
        config = context.config
        target_pct = getattr(config, "target_pct", None)
        sl_pct = getattr(config, "sl_pct", None)
        if not isinstance(target_pct, Decimal) or not isinstance(sl_pct, Decimal):
            raise TypeError(
                "StrategyContext.config must expose target_pct and sl_pct as "
                f"Decimal to build a CombinedPremiumTracker; got config type "
                f"{type(config).__name__}"
            )
        return cls(
            call_instrument=call_instrument,
            put_instrument=put_instrument,
            target_pct=target_pct,
            sl_pct=sl_pct,
        )

    @property
    def call_instrument(self) -> InstrumentIdentifier:
        return self._call_instrument

    @property
    def put_instrument(self) -> InstrumentIdentifier:
        return self._put_instrument

    @property
    def thresholds(self) -> PremiumThresholds | None:
        """Entry/target/stoploss levels, once ``record_entry`` has been
        called -- ``None`` before entry is recorded."""
        return self._thresholds

    @property
    def latest_call_price(self) -> Decimal | None:
        return self._latest_call_price

    @property
    def latest_put_price(self) -> Decimal | None:
        return self._latest_put_price

    def record_entry(
        self, *, call_fill_price: Decimal, put_fill_price: Decimal, at: datetime
    ) -> PremiumThresholds:
        """Record the actual entry fill prices and compute target/stoploss
        from them.

        This is the *entry premium* per the functional spec -- CE fill price
        + PE fill price -- not a value derived from ticks; it is supplied by
        the caller (fill_manager/entry_logic) once both legs' broker fills are
        known. Also seeds the tracker's live prices with the fill prices, so
        ``current_snapshot()`` returns a sane value immediately, even before
        the first post-entry tick arrives. ``at`` is the caller-supplied
        entry timestamp (e.g. when both fills were confirmed) rather than a
        wall-clock read inside this method, keeping it deterministic and
        consistent with the platform's injected-clock convention.

        Raises ``ValueError`` if called more than once on the same tracker --
        one tracker instance corresponds to exactly one entry event.
        """
        if self._thresholds is not None:
            raise ValueError("record_entry() has already been called on this tracker")

        entry_premium = compute_combined_premium(call_fill_price, put_fill_price)
        self._thresholds = compute_thresholds(entry_premium, self._target_pct, self._sl_pct)
        self._latest_call_price = call_fill_price
        self._latest_put_price = put_fill_price
        self._latest_price_at = at
        return self._thresholds

    def on_tick(self, tick: Tick) -> PremiumSnapshot | None:
        """Update whichever leg ``tick`` belongs to and return a fresh
        combined-premium snapshot.

        Returns ``None`` if the tick belongs to neither tracked instrument
        (safe to feed every incoming tick without pre-filtering by instrument)
        or if the sibling leg's price is not yet known (e.g. only one leg has
        ticked so far). Uses the tick's own exchange-reported timestamp, never
        a local wall-clock read, so the snapshot's ``as_of`` reflects when the
        market actually moved, not when this process happened to process it.
        """
        if tick.instrument == self._call_instrument:
            self._latest_call_price = tick.last_price
            self._latest_price_at = tick.timestamp
        elif tick.instrument == self._put_instrument:
            self._latest_put_price = tick.last_price
            self._latest_price_at = tick.timestamp
        else:
            return None

        return self.current_snapshot()

    def current_snapshot(self) -> PremiumSnapshot | None:
        """The latest known combined premium, or ``None`` if either leg's
        price is not yet known (before entry, or before both legs have priced
        at least once)."""
        if self._latest_call_price is None or self._latest_put_price is None:
            return None
        assert self._latest_price_at is not None  # set whenever both prices are

        combined = compute_combined_premium(self._latest_call_price, self._latest_put_price)
        return PremiumSnapshot(
            combined_premium=combined,
            ce_price=self._latest_call_price,
            pe_price=self._latest_put_price,
            as_of=self._latest_price_at,
        )
