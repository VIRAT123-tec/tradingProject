"""Wiring for paper mode's ``PaperTradingBroker`` (see
``brokers/paper_trading_broker.py`` for the composed broker itself).

This module's only job is construction: build a real, read-only ``KiteBroker``
(reusing ``configs/brokers.yaml``'s ``kite:`` settings and rate limits --
identical to how ``DependencyContainer._build_kite_broker`` builds the live
one, since resolving contracts identically is the whole point), a
``SimulationBroker`` whose fill prices come from that same real Kite feed
(``KiteLtpPriceSource``), and composes them into one ``PaperTradingBroker``.

Kept out of ``dependency_container.py`` (and out of ``start_paper.py``
itself) so neither needs to change: ``DependencyContainer.__init__`` already
accepts a pre-built ``broker`` override for exactly this kind of external
composition (see its own docstring), so ``start_paper.py`` only needs to
build one and pass it through.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

from algo.brokers.exceptions import BrokerAuthenticationError
from algo.brokers.paper_trading_broker import KiteLtpPriceSource, PaperTradingBroker
from algo.brokers.rate_limiter import RateLimitConfig, RateLimitedBroker, RateLimitRule
from algo.brokers.simulation import InstrumentCatalog, SimulationBroker, SimulationConfig
from algo.dependency_container import BrokersConfig, KiteBrokerSettings
from algo.strategy_engine.parameter_loader import ParameterLoader

if TYPE_CHECKING:
    from pathlib import Path

    from algo.brokers.kite.kite_auth import AccessTokenStore

__all__ = ["build_paper_trading_broker"]


def _require_env(var_name: str) -> str:
    value = os.environ.get(var_name)
    if not value:
        raise BrokerAuthenticationError(f"required environment variable {var_name!r} is not set")
    return value


def _build_read_only_kite_broker(kite_config: KiteBrokerSettings, access_token_store: AccessTokenStore):
    """Build a real ``KiteBroker`` for market-data reads only -- its own
    order-update websocket is constructed (``KiteBroker`` requires one) but
    is never connected; ``PaperTradingBroker`` routes all order-update wiring
    through the Simulation broker instead. Mirrors
    ``DependencyContainer._build_kite_broker`` exactly, since resolving
    contracts through the identical code path is the whole point of this
    module -- duplicated here (not imported from there) because that method
    is private to the container and paper mode needs this broker built
    *before* the container exists.
    """
    from kiteconnect import KiteConnect

    from algo.brokers.kite.kite_auth import KiteSession
    from algo.brokers.kite.kite_broker import KiteBroker, KiteBrokerConfig
    from algo.brokers.kite.websocket import KiteOrderUpdateStream

    api_key = _require_env(kite_config.api_key_env_var)
    api_secret = _require_env(kite_config.api_secret_env_var)

    client = KiteConnect(api_key=api_key, timeout=kite_config.request_timeout_seconds)
    session = KiteSession(client=client, api_secret=api_secret, token_store=access_token_store)

    def ticker_factory():
        from kiteconnect import KiteTicker

        token = access_token_store.get_access_token()
        if not token:
            raise BrokerAuthenticationError("no Kite access token available to open the order-update stream")
        return KiteTicker(api_key=api_key, access_token=token)

    order_stream = KiteOrderUpdateStream(ticker_factory=ticker_factory)
    return KiteBroker(
        client=client,
        session=session,
        order_stream=order_stream,
        config=KiteBrokerConfig(
            read_retry_attempts=kite_config.read_retry_attempts,
            read_retry_delay_seconds=kite_config.read_retry_delay_seconds,
            quote_batch_size=kite_config.quote_batch_size,
        ),
    )


def build_paper_trading_broker(
    *, access_token_store: AccessTokenStore, config_root: Path | None = None,
) -> PaperTradingBroker:
    """Build paper mode's composed broker: real Kite reads, simulated writes.

    Loads ``configs/brokers.yaml`` independently (the same file
    ``DependencyContainer`` loads, read a second time here since this runs
    *before* the container exists) for the Kite read-delegate's retry/rate-
    limit tuning and the Simulation broker's own tuning
    (``brokers.yaml``'s ``simulation:`` block) -- nothing here is hardcoded
    that config already governs.

    Calls ``load_dotenv()`` itself (matching ``database.py``'s and
    ``scripts/generate_token.py``'s own defensive pattern): this runs from
    ``build_seams()``, called *before* ``DependencyContainer.__init__`` has
    had a chance to load ``.env`` on its own, so ``KITE_API_KEY``/
    ``KITE_API_SECRET`` would otherwise appear unset even when ``.env`` has
    them.
    """
    from dotenv import load_dotenv

    load_dotenv()

    loader = ParameterLoader(config_root)
    brokers_config = loader.load(loader.config_root / "brokers.yaml", BrokersConfig)

    kite = _build_read_only_kite_broker(brokers_config.kite, access_token_store)
    rate_limit_config = RateLimitConfig(
        rules={
            category: RateLimitRule(max_calls=rule.max_calls, per_seconds=rule.per_seconds)
            for category, rule in brokers_config.rate_limits.items()
        }
    )
    kite = RateLimitedBroker(kite, rate_limit_config)

    catalog = InstrumentCatalog()
    simulation = SimulationBroker(
        instrument_catalog=catalog,
        price_source=KiteLtpPriceSource(kite),
        config=SimulationConfig(
            synchronous=brokers_config.simulation.synchronous,
            initial_cash=brokers_config.simulation.initial_cash,
            connection_failure_probability=brokers_config.simulation.connection_failure_probability,
            rejection_probability=brokers_config.simulation.rejection_probability,
            partial_fill_probability=brokers_config.simulation.partial_fill_probability,
            ack_latency_seconds=brokers_config.simulation.ack_latency_seconds,
            fill_latency_seconds=brokers_config.simulation.fill_latency_seconds,
        ),
    )

    return PaperTradingBroker(kite=kite, simulation=simulation, catalog=catalog)
