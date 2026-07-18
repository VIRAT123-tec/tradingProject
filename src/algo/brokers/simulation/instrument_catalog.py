"""In-memory instrument catalog for SimulationBroker.

Injected rather than built into SimulationBroker itself, so tests can seed
exactly the contracts a scenario needs without depending on a real Kite
instrument dump -- the same separation of concerns as PriceSource.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from algo.brokers.broker_base import BrokerInstrument
from algo.brokers.exceptions import InstrumentNotFoundError
from algo.common.enums import Exchange, OptionType

_OptionKey = tuple[str, date, Decimal, OptionType, Exchange]


class InstrumentCatalog:
    """Lookup by tradingsymbol, and by logical option parameters
    (underlying/expiry/strike/option_type) -- the two ways BrokerBase needs
    to resolve an instrument."""

    def __init__(self, instruments: list[BrokerInstrument] | None = None) -> None:
        self._by_symbol: dict[tuple[Exchange, str], BrokerInstrument] = {}
        self._by_option_key: dict[_OptionKey, BrokerInstrument] = {}
        for instrument in instruments or []:
            self.add(instrument)

    def add(self, instrument: BrokerInstrument) -> None:
        """Register a plain instrument, lookup by tradingsymbol only."""
        self._by_symbol[(instrument.exchange, instrument.tradingsymbol)] = instrument

    def add_option(self, *, underlying: str, instrument: BrokerInstrument) -> None:
        """Register an option instrument, lookup by both tradingsymbol and
        (underlying, expiry, strike, option_type)."""
        if instrument.expiry is None or instrument.strike is None or instrument.option_type is None:
            raise ValueError(
                "add_option requires an option instrument (expiry/strike/option_type set)"
            )
        self.add(instrument)
        key: _OptionKey = (
            underlying,
            instrument.expiry,
            instrument.strike,
            instrument.option_type,
            instrument.exchange,
        )
        self._by_option_key[key] = instrument

    def get_by_symbol(self, exchange: Exchange, tradingsymbol: str) -> BrokerInstrument:
        try:
            return self._by_symbol[(exchange, tradingsymbol)]
        except KeyError:
            raise InstrumentNotFoundError(
                f"No instrument {exchange.value}:{tradingsymbol} in the simulated catalog"
            ) from None

    def find_option(
        self,
        *,
        underlying: str,
        expiry: date,
        strike: Decimal,
        option_type: OptionType,
        exchange: Exchange,
    ) -> BrokerInstrument:
        key: _OptionKey = (underlying, expiry, strike, option_type, exchange)
        try:
            return self._by_option_key[key]
        except KeyError:
            raise InstrumentNotFoundError(
                f"No {option_type.value} option for {underlying} strike={strike} "
                f"expiry={expiry} in the simulated catalog"
            ) from None

    @classmethod
    def build_option_chain(
        cls,
        *,
        underlying: str,
        exchange: Exchange,
        expiry: date,
        atm_strike: Decimal,
        strike_interval: Decimal,
        num_strikes_each_side: int,
        lot_size: int,
        tick_size: Decimal = Decimal("0.05"),
        starting_instrument_token: int = 1_000_000,
    ) -> "InstrumentCatalog":
        """Convenience factory: builds a full CE/PE chain centered on
        atm_strike, so a test or paper-trading setup doesn't need to
        hand-author every contract to exercise a straddle strategy."""
        catalog = cls()
        token = starting_instrument_token
        for i in range(-num_strikes_each_side, num_strikes_each_side + 1):
            strike = atm_strike + strike_interval * i
            for option_type in (OptionType.CE, OptionType.PE):
                symbol = (
                    f"{underlying}{expiry.strftime('%y%b%d').upper()}"
                    f"{int(strike)}{option_type.value}"
                )
                instrument = BrokerInstrument(
                    instrument_token=token,
                    exchange=exchange,
                    tradingsymbol=symbol,
                    name=underlying,
                    lot_size=lot_size,
                    tick_size=tick_size,
                    expiry=expiry,
                    strike=strike,
                    option_type=option_type,
                )
                catalog.add_option(underlying=underlying, instrument=instrument)
                token += 1
        return catalog
