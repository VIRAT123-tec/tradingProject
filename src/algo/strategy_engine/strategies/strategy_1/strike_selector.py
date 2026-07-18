"""Computes the ATM strike from the underlying spot LTP, rounded to the
instrument's strike interval (from config, via InstrumentService -- never
hardcoded), and resolves the current weekly expiry's CE/PE trading symbols via
ExpiryService and the already-established broker abstraction's
``find_option_contract``.

Scope boundary: this module answers exactly one question -- "which two option
contracts constitute today's ATM straddle for this instrument?" -- and nothing
else. It does not decide *when* to enter, does not decide whether to trade a
same-day (0DTE) expiry, does not place orders, and does not compute premium or
P&L. Those are entry_logic.py's, exit_logic.py's, and combined_premium.py's
concerns respectively.

Money-critical arithmetic: the ATM rounding itself is isolated in a pure,
module-level function (``compute_atm_strike``) with no I/O, so it can be
unit-tested exhaustively (every rounding edge case) independent of the
broker/service calls the rest of this module makes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import ROUND_HALF_UP, Decimal
from typing import TYPE_CHECKING

from algo.brokers.exceptions import InstrumentNotFoundError
from algo.common.enums import Exchange, OptionType

if TYPE_CHECKING:
    from algo.brokers.broker_base import BrokerBase, BrokerInstrument
    from algo.services.expiry_service import ExpiryService
    from algo.services.instrument_service import InstrumentService


class StrikeSelectionError(Exception):
    """Raised when today's ATM straddle contracts cannot be resolved.

    Wraps lower-level failures (an unknown instrument, a broker instrument
    lookup miss, a resolved contract that doesn't match what was requested)
    into one exception type so callers need to catch only this, not the
    broker layer's or the services layer's separate vocabularies.
    """


@dataclass(frozen=True, slots=True)
class StraddleStrikeSelection:
    """The resolved ATM straddle for one instrument on one trading day.

    Every field here is either an input snapshot (``spot_ltp``,
    ``strike_interval``) or a value independently confirmed against the
    broker's own response (``call``/``put`` are validated to actually carry
    ``atm_strike``/``expiry`` before this object is constructed -- see
    ``StrikeSelector._validate_resolved_contract``).
    """

    instrument: str
    exchange: Exchange
    spot_ltp: Decimal
    strike_interval: Decimal
    atm_strike: Decimal
    expiry: date
    call: BrokerInstrument
    put: BrokerInstrument


def compute_atm_strike(spot_ltp: Decimal, strike_interval: Decimal) -> Decimal:
    """Round ``spot_ltp`` to the nearest multiple of ``strike_interval``.

    Pure function, no I/O -- the single piece of arithmetic this whole module
    exists to get right, kept separately and exhaustively unit-testable from
    the broker/service-calling ``StrikeSelector.select``.

    Ties round up (e.g. spot exactly halfway between two strikes rounds to the
    higher strike): ``ROUND_HALF_UP`` on the quotient before rescaling, which is
    the conventional "round half away from zero for positive input" behavior
    expected of ATM strike selection. Both inputs must be positive; a spot LTP
    of zero or a non-positive strike interval indicates bad input upstream
    (a market-data fault or a broken config) that must not be silently rounded
    into a nonsensical strike.
    """
    if spot_ltp <= 0:
        raise StrikeSelectionError(f"spot_ltp must be positive, got {spot_ltp}")
    if strike_interval <= 0:
        raise StrikeSelectionError(
            f"strike_interval must be positive, got {strike_interval}"
        )

    quotient = (spot_ltp / strike_interval).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
    return quotient * strike_interval


class StrikeSelector:
    """Resolves today's ATM straddle (CE + PE) for one instrument.

    Depends only on the ``InstrumentService``/``ExpiryService`` Protocol seams
    and the already-approved ``BrokerBase`` abstraction -- never on YAML paths,
    a specific broker implementation, or a hardcoded expiry weekday. This is
    what keeps the module usable identically for Nifty and Sensex, and
    identically against the simulation and (later) Kite brokers.
    """

    def __init__(
        self,
        *,
        instrument_service: InstrumentService,
        expiry_service: ExpiryService,
        broker: BrokerBase,
    ) -> None:
        self._instrument_service = instrument_service
        self._expiry_service = expiry_service
        self._broker = broker

    def select(
        self, *, instrument: str, spot_ltp: Decimal, as_of: date
    ) -> StraddleStrikeSelection:
        """Resolve today's ATM straddle for ``instrument`` given its current
        ``spot_ltp`` and the trading date ``as_of``.

        Sequence: look up the instrument's static spec (exchange, strike
        interval) -> compute the ATM strike -> ask ExpiryService for the
        current weekly expiry -> ask the broker to resolve the CE and PE
        contracts for that (underlying, expiry, strike) -> verify the broker's
        answers actually match what was requested before returning them.

        Raises ``StrikeSelectionError`` for any failure in this chain
        (unknown instrument, expiry resolution failure, no matching option
        contract, or a resolved contract that doesn't match the request) --
        callers need to handle only this one exception type.
        """
        spec = self._instrument_service.get_instrument_spec(instrument)
        atm_strike = compute_atm_strike(spot_ltp, spec.strike_interval)
        expiry = self._expiry_service.get_current_weekly_expiry(instrument, as_of)
        # The name the broker's contracts carry may differ from the display
        # identity (e.g. SENSEXBANK -> BANKEX); default to the identity.
        underlying = spec.underlying_symbol or instrument

        call = self._resolve_contract(
            instrument=instrument,
            underlying=underlying,
            expiry=expiry,
            strike=atm_strike,
            option_type=OptionType.CE,
            exchange=spec.exchange,
        )
        put = self._resolve_contract(
            instrument=instrument,
            underlying=underlying,
            expiry=expiry,
            strike=atm_strike,
            option_type=OptionType.PE,
            exchange=spec.exchange,
        )

        return StraddleStrikeSelection(
            instrument=instrument,
            exchange=spec.exchange,
            spot_ltp=spot_ltp,
            strike_interval=spec.strike_interval,
            atm_strike=atm_strike,
            expiry=expiry,
            call=call,
            put=put,
        )

    # -- Internal ------------------------------------------------------

    def _resolve_contract(
        self,
        *,
        instrument: str,
        underlying: str,
        expiry: date,
        strike: Decimal,
        option_type: OptionType,
        exchange: Exchange,
    ) -> BrokerInstrument:
        """Ask the broker for one leg's contract and verify its answer before
        trusting it. ``underlying`` is the broker-dump contract name (usually
        equal to ``instrument``); ``instrument`` remains the display identity
        used in error messages."""
        try:
            resolved = self._broker.find_option_contract(
                underlying=underlying,
                expiry=expiry,
                strike=strike,
                option_type=option_type,
                exchange=exchange,
            )
        except InstrumentNotFoundError as exc:
            raise StrikeSelectionError(
                f"no {option_type.value} contract found for {instrument} "
                f"strike={strike} expiry={expiry} exchange={exchange.value}"
            ) from exc

        self._validate_resolved_contract(
            resolved,
            expected_strike=strike,
            expected_expiry=expiry,
            expected_option_type=option_type,
            expected_exchange=exchange,
        )
        return resolved

    @staticmethod
    def _validate_resolved_contract(
        resolved: BrokerInstrument,
        *,
        expected_strike: Decimal,
        expected_expiry: date,
        expected_option_type: OptionType,
        expected_exchange: Exchange,
    ) -> None:
        """Defensive check: never silently trust a broker's answer for a
        money-moving lookup. A mismatch here means a broken concrete broker
        implementation (or its mapper), not a normal runtime condition -- it
        must surface loudly rather than let a wrong-strike order be placed.
        """
        mismatches: list[str] = []
        if resolved.strike != expected_strike:
            mismatches.append(f"strike: expected {expected_strike}, got {resolved.strike}")
        if resolved.expiry != expected_expiry:
            mismatches.append(f"expiry: expected {expected_expiry}, got {resolved.expiry}")
        if resolved.option_type != expected_option_type:
            mismatches.append(
                f"option_type: expected {expected_option_type.value}, "
                f"got {resolved.option_type}"
            )
        if resolved.exchange != expected_exchange:
            mismatches.append(
                f"exchange: expected {expected_exchange.value}, got {resolved.exchange}"
            )
        if mismatches:
            raise StrikeSelectionError(
                f"broker resolved {resolved.tradingsymbol!r} but it does not match "
                f"the request: {'; '.join(mismatches)}"
            )
