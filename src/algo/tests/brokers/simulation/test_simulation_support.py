"""Tests for InstrumentCatalog and the PriceSource implementations --
SimulationBroker's two injected data collaborators."""

from __future__ import annotations

import random
from datetime import date
from decimal import Decimal

import pytest

from algo.brokers import InstrumentIdentifier
from algo.brokers.exceptions import InstrumentNotFoundError
from algo.brokers.simulation import InstrumentCatalog, RandomWalkPriceSource, StaticPriceSource
from algo.common.enums import Exchange, OptionType


class TestInstrumentCatalog:
    def test_build_option_chain_covers_every_strike_and_side(self):
        catalog = InstrumentCatalog.build_option_chain(
            underlying="NIFTY",
            exchange=Exchange.NFO,
            expiry=date(2026, 7, 30),
            atm_strike=Decimal("24000"),
            strike_interval=Decimal("50"),
            num_strikes_each_side=3,
            lot_size=75,
        )
        # 3 each side + ATM itself = 7 strikes, x2 for CE/PE = 14 instruments.
        for offset in (-3, -2, -1, 0, 1, 2, 3):
            for option_type in (OptionType.CE, OptionType.PE):
                strike = Decimal("24000") + Decimal("50") * offset
                instrument = catalog.find_option(
                    underlying="NIFTY", expiry=date(2026, 7, 30), strike=strike,
                    option_type=option_type, exchange=Exchange.NFO,
                )
                assert instrument.strike == strike
                assert instrument.option_type == option_type
                assert instrument.lot_size == 75

    def test_instrument_tokens_are_unique(self):
        catalog = InstrumentCatalog.build_option_chain(
            underlying="NIFTY", exchange=Exchange.NFO, expiry=date(2026, 7, 30),
            atm_strike=Decimal("24000"), strike_interval=Decimal("50"),
            num_strikes_each_side=5, lot_size=75,
        )
        tokens = [i.instrument_token for i in catalog._by_symbol.values()]
        assert len(tokens) == len(set(tokens))

    def test_get_by_symbol_round_trips(self):
        catalog = InstrumentCatalog.build_option_chain(
            underlying="NIFTY", exchange=Exchange.NFO, expiry=date(2026, 7, 30),
            atm_strike=Decimal("24000"), strike_interval=Decimal("50"),
            num_strikes_each_side=1, lot_size=75,
        )
        by_option = catalog.find_option(
            underlying="NIFTY", expiry=date(2026, 7, 30), strike=Decimal("24000"),
            option_type=OptionType.CE, exchange=Exchange.NFO,
        )
        by_symbol = catalog.get_by_symbol(Exchange.NFO, by_option.tradingsymbol)
        assert by_symbol == by_option

    def test_unknown_symbol_raises_not_found(self):
        catalog = InstrumentCatalog()
        with pytest.raises(InstrumentNotFoundError):
            catalog.get_by_symbol(Exchange.NFO, "DOES_NOT_EXIST")

    def test_unknown_option_raises_not_found(self):
        catalog = InstrumentCatalog.build_option_chain(
            underlying="NIFTY", exchange=Exchange.NFO, expiry=date(2026, 7, 30),
            atm_strike=Decimal("24000"), strike_interval=Decimal("50"),
            num_strikes_each_side=1, lot_size=75,
        )
        with pytest.raises(InstrumentNotFoundError):
            catalog.find_option(
                underlying="NIFTY", expiry=date(2026, 7, 30), strike=Decimal("99999"),
                option_type=OptionType.CE, exchange=Exchange.NFO,
            )

    def test_add_option_rejects_non_option_instrument(self):
        from algo.brokers import BrokerInstrument

        catalog = InstrumentCatalog()
        plain = BrokerInstrument(
            instrument_token=1, exchange=Exchange.NFO, tradingsymbol="NIFTYFUT",
            name="NIFTY", lot_size=75, tick_size=Decimal("0.05"),
        )
        with pytest.raises(ValueError):
            catalog.add_option(underlying="NIFTY", instrument=plain)


class TestStaticPriceSource:
    def test_returns_configured_price(self):
        ii = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="X")
        source = StaticPriceSource({ii: Decimal("100")})
        assert source.get_ltp(ii) == Decimal("100")

    def test_unconfigured_instrument_raises_key_error(self):
        source = StaticPriceSource({})
        ii = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="X")
        with pytest.raises(KeyError):
            source.get_ltp(ii)

    def test_set_price_mutates_subsequent_reads(self):
        ii = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="X")
        source = StaticPriceSource({ii: Decimal("100")})
        source.set_price(ii, Decimal("150"))
        assert source.get_ltp(ii) == Decimal("150")


class TestRandomWalkPriceSource:
    def test_same_seed_produces_identical_price_sequence(self):
        ii = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="X")
        source_a = RandomWalkPriceSource({ii: Decimal("100")}, rng=random.Random(7))
        source_b = RandomWalkPriceSource({ii: Decimal("100")}, rng=random.Random(7))

        sequence_a = [source_a.get_ltp(ii) for _ in range(10)]
        sequence_b = [source_b.get_ltp(ii) for _ in range(10)]

        assert sequence_a == sequence_b

    def test_price_stays_positive_under_repeated_walk(self):
        ii = InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol="X")
        source = RandomWalkPriceSource({ii: Decimal("0.10")}, rng=random.Random(1), volatility=Decimal("0.5"))
        for _ in range(200):
            price = source.get_ltp(ii)
            assert price > Decimal("0")
