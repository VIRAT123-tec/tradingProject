"""AtmWindowManager: keeps each underlying's ATM ±N strike window centred on
spot, computing the *minimal* subscription change each cycle.

For each underlying it reads spot, computes ATM (reusing the trading platform's
pure ``compute_atm_strike``), resolves the current expiry (reused
``ExpiryService``), enumerates ATM ±N strikes' CE/PE from the option chain, and
**diffs against the currently-subscribed set** -- so when spot crosses a strike
the manager returns only the new edge to subscribe and the old edge to
unsubscribe, never the whole window. It also returns the identity of newly-added
instruments so the caller can upsert the dimension table.

Pure orchestration of injected collaborators (spot source, chain source,
instrument/expiry services) -- no I/O of its own, fully unit-testable.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Callable

from algo.common.enums import Exchange, OptionType
from algo.strategy_engine.strategies.strategy_1.strike_selector import compute_atm_strike

if TYPE_CHECKING:
    from algo.market_data_collector.instrument_chain import OptionChain
    from algo.services.live_seams import ConfigInstrumentService

    ExpiryResolver = Callable[[str, date], date]
    SpotSource = Callable[[str], Decimal]
    ChainSource = Callable[[str, date], OptionChain]


@dataclass(frozen=True, slots=True)
class InstrumentRef:
    """Identity of one subscribed option contract (for the dimension table)."""

    instrument_token: int
    underlying: str
    exchange: Exchange
    expiry: date
    strike: Decimal
    option_type: OptionType
    tradingsymbol: str


@dataclass(frozen=True, slots=True)
class WindowDiff:
    underlying: str
    atm: Decimal
    to_subscribe: list[int]
    to_unsubscribe: list[int]
    new_instruments: list[InstrumentRef] = field(default_factory=list)


class AtmWindowManager:
    """Tracks and rotates each underlying's ATM ±N subscription window."""

    def __init__(
        self,
        *,
        instrument_service: ConfigInstrumentService,
        expiry_resolver: ExpiryResolver,
        spot_source: SpotSource,
        chain_source: ChainSource,
        strikes_each_side: int,
        logger=None,
    ) -> None:
        self._instruments = instrument_service
        self._resolve_expiry = expiry_resolver
        self._spot = spot_source
        self._chain = chain_source
        self._n = strikes_each_side
        import logging
        self._logger = logger or logging.getLogger("algo.collector.atm")
        # underlying -> {token -> InstrumentRef} currently subscribed.
        self._current: dict[str, dict[int, InstrumentRef]] = {}
        self._atm: dict[str, Decimal] = {}

    def reset(self) -> None:
        """Forget all current windows -- used when a new session starts after the
        socket was disconnected (so the next recompute re-subscribes fresh)."""
        self._current.clear()
        self._atm.clear()

    def current_atm(self, underlying: str) -> Decimal | None:
        return self._atm.get(underlying)

    def subscribed_tokens(self) -> set[int]:
        return {tok for legs in self._current.values() for tok in legs}

    def instrument_ref(self, token: int) -> InstrumentRef | None:
        for legs in self._current.values():
            ref = legs.get(token)
            if ref is not None:
                return ref
        return None

    def recompute(self, underlying: str, as_of: date) -> WindowDiff:
        """Recompute the window for one underlying and return the minimal diff.
        Raises only if spot/spec cannot be obtained (caller handles/logs)."""
        spec = self._instruments.get_instrument_spec(underlying)
        spot = self._spot(underlying)
        atm = compute_atm_strike(spot, spec.strike_interval)
        expiry = self._resolve_expiry(underlying, as_of)
        chain = self._chain(underlying, expiry)

        target: dict[int, InstrumentRef] = {}
        for k in range(-self._n, self._n + 1):
            strike = atm + spec.strike_interval * k
            legs = chain.get(strike)
            if not legs:
                self._logger.debug(
                    "%s: no chain entry at strike %s (expiry %s); skipping", underlying, strike, expiry
                )
                continue
            for opt in (OptionType.CE, OptionType.PE):
                leg = legs.get(opt.value)
                if leg is None:
                    continue
                symbol, token = leg
                target[token] = InstrumentRef(
                    instrument_token=token, underlying=underlying, exchange=spec.exchange,
                    expiry=expiry, strike=strike, option_type=opt, tradingsymbol=symbol,
                )

        current = self._current.get(underlying, {})
        to_subscribe = [tok for tok in target if tok not in current]
        to_unsubscribe = [tok for tok in current if tok not in target]
        new_instruments = [target[tok] for tok in to_subscribe]

        self._current[underlying] = target
        self._atm[underlying] = atm
        return WindowDiff(
            underlying=underlying, atm=atm,
            to_subscribe=to_subscribe, to_unsubscribe=to_unsubscribe,
            new_instruments=new_instruments,
        )
