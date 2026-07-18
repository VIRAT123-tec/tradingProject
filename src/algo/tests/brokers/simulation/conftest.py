"""Shared fixtures for SimulationBroker tests.

A small, fixed NIFTY option chain (one expiry, ATM +/- 5 strikes) is built
once per test via `catalog`, with `ce_identifier`/`pe_identifier` resolving
its ATM CE/PE legs -- enough to exercise a straddle-shaped order flow without
every test hand-authoring instruments.
"""

from __future__ import annotations

import random
from datetime import date
from decimal import Decimal

import pytest

from algo.brokers import InstrumentIdentifier, PlaceOrderRequest
from algo.brokers.simulation import InstrumentCatalog, SimulationBroker, SimulationConfig, StaticPriceSource
from algo.common.enums import Exchange, OptionType, OrderType, ProductType, TransactionType

UNDERLYING = "NIFTY"
EXPIRY = date(2026, 7, 30)
ATM_STRIKE = Decimal("24000")
STRIKE_INTERVAL = Decimal("50")
LOT_SIZE = 75

CE_LTP = Decimal("120.50")
PE_LTP = Decimal("115.25")


@pytest.fixture
def catalog() -> InstrumentCatalog:
    return InstrumentCatalog.build_option_chain(
        underlying=UNDERLYING,
        exchange=Exchange.NFO,
        expiry=EXPIRY,
        atm_strike=ATM_STRIKE,
        strike_interval=STRIKE_INTERVAL,
        num_strikes_each_side=5,
        lot_size=LOT_SIZE,
    )


@pytest.fixture
def ce_symbol(catalog: InstrumentCatalog) -> str:
    instrument = catalog.find_option(
        underlying=UNDERLYING,
        expiry=EXPIRY,
        strike=ATM_STRIKE,
        option_type=OptionType.CE,
        exchange=Exchange.NFO,
    )
    return instrument.tradingsymbol


@pytest.fixture
def pe_symbol(catalog: InstrumentCatalog) -> str:
    instrument = catalog.find_option(
        underlying=UNDERLYING,
        expiry=EXPIRY,
        strike=ATM_STRIKE,
        option_type=OptionType.PE,
        exchange=Exchange.NFO,
    )
    return instrument.tradingsymbol


@pytest.fixture
def ce_identifier(ce_symbol: str) -> InstrumentIdentifier:
    return InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol=ce_symbol)


@pytest.fixture
def pe_identifier(pe_symbol: str) -> InstrumentIdentifier:
    return InstrumentIdentifier(exchange=Exchange.NFO, tradingsymbol=pe_symbol)


@pytest.fixture
def price_source(ce_identifier: InstrumentIdentifier, pe_identifier: InstrumentIdentifier) -> StaticPriceSource:
    return StaticPriceSource({ce_identifier: CE_LTP, pe_identifier: PE_LTP})


@pytest.fixture
def broker_factory(catalog: InstrumentCatalog, price_source: StaticPriceSource):
    """Factory for building authenticated SimulationBroker instances; every
    broker it creates is close()d at teardown so background matching-engine
    threads never leak across tests."""
    created: list[SimulationBroker] = []

    def _factory(
        *,
        config: SimulationConfig | None = None,
        seed: int = 0,
        authenticate: bool = True,
    ) -> SimulationBroker:
        broker = SimulationBroker(
            instrument_catalog=catalog,
            price_source=price_source,
            config=config or SimulationConfig(synchronous=True),
            rng=random.Random(seed),
        )
        created.append(broker)
        if authenticate:
            broker.authenticate()
        return broker

    yield _factory

    for broker in created:
        broker.close()


def make_request(
    symbol: str,
    *,
    tag: str,
    quantity: int = LOT_SIZE,
    transaction_type: TransactionType = TransactionType.SELL,
    product: ProductType = ProductType.INTRADAY,
    order_type: OrderType = OrderType.MARKET,
    exchange: Exchange = Exchange.NFO,
) -> PlaceOrderRequest:
    return PlaceOrderRequest(
        exchange=exchange,
        tradingsymbol=symbol,
        transaction_type=transaction_type,
        quantity=quantity,
        product=product,
        order_type=order_type,
        tag=tag,
    )
