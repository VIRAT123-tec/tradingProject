"""Unit tests for the Strategy-1 strike selector.

Flagged in the spec as a piece most likely to have subtle, money-costing bugs.
``compute_atm_strike`` is tested exhaustively in isolation (no I/O).
``StrikeSelector.select`` is tested against the real, already-verified
``SimulationBroker`` + ``InstrumentCatalog`` (not a hand-rolled broker fake) for
the happy paths, plus small purpose-built fakes for the error/mismatch paths
that a real broker would never actually produce.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import pytest

from algo.brokers.broker_base import BrokerInstrument
from algo.brokers.simulation import InstrumentCatalog, SimulationBroker, SimulationConfig
from algo.common.enums import Exchange, OptionType
from algo.services.instrument_service import InstrumentSpec
from algo.strategy_engine.strategies.strategy_1.strike_selector import (
    StrikeSelectionError,
    StrikeSelector,
    compute_atm_strike,
)

NIFTY_EXPIRY = date(2026, 7, 30)
SENSEX_EXPIRY = date(2026, 7, 31)


# --------------------------------------------------------------------------
# compute_atm_strike: pure, exhaustive
# --------------------------------------------------------------------------


class TestComputeAtmStrike:
    def test_exact_multiple_is_unchanged(self):
        assert compute_atm_strike(Decimal("24000"), Decimal("50")) == Decimal("24000")

    def test_rounds_down_when_closer_to_lower_strike(self):
        assert compute_atm_strike(Decimal("24010"), Decimal("50")) == Decimal("24000")

    def test_rounds_up_when_closer_to_higher_strike(self):
        assert compute_atm_strike(Decimal("24040"), Decimal("50")) == Decimal("24050")

    def test_exact_tie_rounds_up(self):
        # 24025 is exactly halfway between 24000 and 24050.
        assert compute_atm_strike(Decimal("24025"), Decimal("50")) == Decimal("24050")

    def test_sensex_interval_of_100(self):
        assert compute_atm_strike(Decimal("81234"), Decimal("100")) == Decimal("81200")
        assert compute_atm_strike(Decimal("81260"), Decimal("100")) == Decimal("81300")
        assert compute_atm_strike(Decimal("81250"), Decimal("100")) == Decimal("81300")  # tie

    def test_small_spot_and_interval(self):
        assert compute_atm_strike(Decimal("101"), Decimal("5")) == Decimal("100")
        assert compute_atm_strike(Decimal("103"), Decimal("5")) == Decimal("105")

    def test_result_is_decimal_and_exact(self):
        result = compute_atm_strike(Decimal("24017.35"), Decimal("50"))
        assert result == Decimal("24000")
        assert isinstance(result, Decimal)

    def test_zero_spot_raises(self):
        with pytest.raises(StrikeSelectionError):
            compute_atm_strike(Decimal("0"), Decimal("50"))

    def test_negative_spot_raises(self):
        with pytest.raises(StrikeSelectionError):
            compute_atm_strike(Decimal("-100"), Decimal("50"))

    def test_zero_interval_raises(self):
        with pytest.raises(StrikeSelectionError):
            compute_atm_strike(Decimal("24000"), Decimal("0"))

    def test_negative_interval_raises(self):
        with pytest.raises(StrikeSelectionError):
            compute_atm_strike(Decimal("24000"), Decimal("-50"))


# --------------------------------------------------------------------------
# Fakes for InstrumentService / ExpiryService
# --------------------------------------------------------------------------


@dataclass
class FakeInstrumentService:
    specs: dict[str, InstrumentSpec]

    def get_instrument_spec(self, instrument: str) -> InstrumentSpec:
        return self.specs[instrument]


@dataclass
class FakeExpiryService:
    expiries: dict[str, date]

    def get_current_weekly_expiry(self, instrument: str, as_of: date) -> date:
        return self.expiries[instrument]


NIFTY_SPEC = InstrumentSpec(
    instrument="NIFTY",
    exchange=Exchange.NFO,
    strike_interval=Decimal("50"),
    lot_size=75,
    tick_size=Decimal("0.05"),
)
SENSEX_SPEC = InstrumentSpec(
    instrument="SENSEX",
    exchange=Exchange.BFO,
    strike_interval=Decimal("100"),
    lot_size=20,
    tick_size=Decimal("0.05"),
)


@pytest.fixture
def instrument_service() -> FakeInstrumentService:
    return FakeInstrumentService(specs={"NIFTY": NIFTY_SPEC, "SENSEX": SENSEX_SPEC})


@pytest.fixture
def expiry_service() -> FakeExpiryService:
    return FakeExpiryService(expiries={"NIFTY": NIFTY_EXPIRY, "SENSEX": SENSEX_EXPIRY})


@pytest.fixture
def nifty_catalog() -> InstrumentCatalog:
    return InstrumentCatalog.build_option_chain(
        underlying="NIFTY",
        exchange=Exchange.NFO,
        expiry=NIFTY_EXPIRY,
        atm_strike=Decimal("24000"),
        strike_interval=Decimal("50"),
        num_strikes_each_side=5,
        lot_size=75,
    )


@pytest.fixture
def sensex_catalog() -> InstrumentCatalog:
    return InstrumentCatalog.build_option_chain(
        underlying="SENSEX",
        exchange=Exchange.BFO,
        expiry=SENSEX_EXPIRY,
        atm_strike=Decimal("81200"),
        strike_interval=Decimal("100"),
        num_strikes_each_side=5,
        lot_size=20,
    )


def _simulation_broker(catalog: InstrumentCatalog) -> SimulationBroker:
    from algo.brokers.broker_base import InstrumentIdentifier
    from algo.brokers.simulation import StaticPriceSource

    # StrikeSelector never calls get_ltp/place_order -- only find_option_contract
    # -- so an empty price source is sufficient here.
    broker = SimulationBroker(
        instrument_catalog=catalog,
        price_source=StaticPriceSource({}),
        config=SimulationConfig(synchronous=True),
        rng=random.Random(0),
    )
    broker.authenticate()
    return broker


# --------------------------------------------------------------------------
# StrikeSelector.select against the real SimulationBroker
# --------------------------------------------------------------------------


class TestStrikeSelectorHappyPath:
    def test_resolves_nifty_atm_straddle(self, instrument_service, expiry_service, nifty_catalog):
        broker = _simulation_broker(nifty_catalog)
        selector = StrikeSelector(
            instrument_service=instrument_service, expiry_service=expiry_service, broker=broker
        )

        selection = selector.select(
            instrument="NIFTY", spot_ltp=Decimal("24017"), as_of=date(2026, 7, 27)
        )

        assert selection.atm_strike == Decimal("24000")
        assert selection.expiry == NIFTY_EXPIRY
        assert selection.exchange == Exchange.NFO
        assert selection.call.option_type == OptionType.CE
        assert selection.put.option_type == OptionType.PE
        assert selection.call.strike == Decimal("24000")
        assert selection.put.strike == Decimal("24000")
        assert selection.call.tradingsymbol != selection.put.tradingsymbol
        broker.close()

    def test_resolves_sensex_atm_straddle_with_different_interval(
        self, instrument_service, expiry_service, sensex_catalog
    ):
        broker = _simulation_broker(sensex_catalog)
        selector = StrikeSelector(
            instrument_service=instrument_service, expiry_service=expiry_service, broker=broker
        )

        selection = selector.select(
            instrument="SENSEX", spot_ltp=Decimal("81234"), as_of=date(2026, 7, 27)
        )

        assert selection.atm_strike == Decimal("81200")  # rounds to nearest 100, not 50
        assert selection.exchange == Exchange.BFO
        assert selection.expiry == SENSEX_EXPIRY
        broker.close()

    def test_moving_spot_price_changes_resolved_strike(
        self, instrument_service, expiry_service, nifty_catalog
    ):
        broker = _simulation_broker(nifty_catalog)
        selector = StrikeSelector(
            instrument_service=instrument_service, expiry_service=expiry_service, broker=broker
        )

        low = selector.select(instrument="NIFTY", spot_ltp=Decimal("23980"), as_of=date(2026, 7, 27))
        high = selector.select(instrument="NIFTY", spot_ltp=Decimal("24080"), as_of=date(2026, 7, 27))

        assert low.atm_strike == Decimal("24000")
        assert high.atm_strike == Decimal("24100")
        assert low.call.tradingsymbol != high.call.tradingsymbol
        broker.close()


class TestStrikeSelectorErrors:
    def test_unknown_instrument_raises_key_error_from_service(
        self, instrument_service, expiry_service, nifty_catalog
    ):
        broker = _simulation_broker(nifty_catalog)
        selector = StrikeSelector(
            instrument_service=instrument_service, expiry_service=expiry_service, broker=broker
        )
        with pytest.raises(KeyError):
            selector.select(instrument="BANKNIFTY", spot_ltp=Decimal("50000"), as_of=date(2026, 7, 27))
        broker.close()

    def test_no_matching_contract_raises_strike_selection_error(
        self, instrument_service, expiry_service
    ):
        # A sparse catalog that doesn't cover the strike the spot resolves to.
        sparse_catalog = InstrumentCatalog.build_option_chain(
            underlying="NIFTY",
            exchange=Exchange.NFO,
            expiry=NIFTY_EXPIRY,
            atm_strike=Decimal("24000"),
            strike_interval=Decimal("50"),
            num_strikes_each_side=1,  # only 23950/24000/24050 exist
            lot_size=75,
        )
        broker = _simulation_broker(sparse_catalog)
        selector = StrikeSelector(
            instrument_service=instrument_service, expiry_service=expiry_service, broker=broker
        )

        with pytest.raises(StrikeSelectionError):
            # 25000 is far outside the sparse chain.
            selector.select(instrument="NIFTY", spot_ltp=Decimal("25000"), as_of=date(2026, 7, 27))
        broker.close()

    def test_mismatched_broker_response_is_rejected(self, instrument_service, expiry_service):
        selector = StrikeSelector(
            instrument_service=instrument_service,
            expiry_service=expiry_service,
            broker=_WrongStrikeBroker(),
        )
        with pytest.raises(StrikeSelectionError, match="does not match"):
            selector.select(instrument="NIFTY", spot_ltp=Decimal("24000"), as_of=date(2026, 7, 27))


class _WrongStrikeBroker:
    """Minimal broker double that answers with the wrong strike, to prove
    StrikeSelector does not blindly trust the broker's response."""

    def find_option_contract(self, *, underlying, expiry, strike, option_type, exchange, timeout=None):
        return BrokerInstrument(
            instrument_token=1,
            exchange=exchange,
            tradingsymbol="WRONG",
            name=underlying,
            lot_size=75,
            tick_size=Decimal("0.05"),
            expiry=expiry,
            strike=strike + Decimal("50"),  # deliberately wrong
            option_type=option_type,
        )


# --------------------------------------------------------------------------
# underlying_symbol override: identity vs. broker-dump contract name
# --------------------------------------------------------------------------


class _RecordingBroker:
    """Captures the ``underlying`` passed to find_option_contract and returns a
    matching contract, so a test can assert which name was used for lookup."""

    def __init__(self) -> None:
        self.underlyings: list[str] = []

    def find_option_contract(self, *, underlying, expiry, strike, option_type, exchange, timeout=None):
        self.underlyings.append(underlying)
        return BrokerInstrument(
            instrument_token=1, exchange=exchange, tradingsymbol=f"{underlying}X",
            name=underlying, lot_size=30, tick_size=Decimal("0.05"),
            expiry=expiry, strike=strike, option_type=option_type,
        )


class TestUnderlyingSymbolOverride:
    def test_lookup_uses_underlying_symbol_but_keeps_identity(self):
        # SENSEXBANK's contracts are named BANKEX in the broker dump.
        spec = InstrumentSpec(
            instrument="SENSEXBANK", exchange=Exchange.BFO, strike_interval=Decimal("100"),
            lot_size=30, tick_size=Decimal("0.05"), underlying_symbol="BANKEX",
        )
        broker = _RecordingBroker()
        selector = StrikeSelector(
            instrument_service=FakeInstrumentService(specs={"SENSEXBANK": spec}),
            expiry_service=FakeExpiryService(expiries={"SENSEXBANK": date(2026, 7, 30)}),
            broker=broker,
        )
        result = selector.select(instrument="SENSEXBANK", spot_ltp=Decimal("65000"), as_of=date(2026, 7, 14))
        # Contract lookup used the mapped Kite name for both legs...
        assert broker.underlyings == ["BANKEX", "BANKEX"]
        # ...but the display identity is preserved on the selection.
        assert result.instrument == "SENSEXBANK"

    def test_lookup_defaults_to_identity_when_no_override(self):
        # Backward compatibility: no underlying_symbol -> identity is used.
        spec = InstrumentSpec(
            instrument="NIFTY", exchange=Exchange.NFO, strike_interval=Decimal("50"),
            lot_size=75, tick_size=Decimal("0.05"),
        )
        broker = _RecordingBroker()
        selector = StrikeSelector(
            instrument_service=FakeInstrumentService(specs={"NIFTY": spec}),
            expiry_service=FakeExpiryService(expiries={"NIFTY": date(2026, 7, 30)}),
            broker=broker,
        )
        selector.select(instrument="NIFTY", spot_ltp=Decimal("24000"), as_of=date(2026, 7, 14))
        assert broker.underlyings == ["NIFTY", "NIFTY"]
