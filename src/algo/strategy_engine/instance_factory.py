"""InstanceFactory: assembles a runnable strategy instance from a strategy id,
an instrument, and an account.

This is the one place the abstract engine meets concrete config, DB identity,
and injected dependencies. Given ("strategy_1", "nifty", account_id, NFO) it:

    1. looks up the strategy class in the registry,
    2. loads + validates that instrument's YAML into the strategy's declared
       config schema (fail-fast at startup),
    3. get-or-creates the persistent ``StrategyInstance`` row (so a restart
       re-attaches to the same identity rather than duplicating it),
    4. builds a tagged logger and the ``StrategyContext``,
    5. constructs the strategy and wraps it in a ``StrategyRunner``.

Two instruments = two ``build_runner`` calls with the same strategy id and
different instrument/config -- no per-instrument code. All shared collaborators
(session factory, broker, market data, risk, time) are injected once into the
factory, keeping the composition graph explicit and testable.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from algo.database.repositories.strategy_instance_repository import (
    StrategyInstanceRepository,
)
from algo.database.session import unit_of_work
from algo.strategy_engine.parameter_loader import ParameterLoader
from algo.strategy_engine.strategy_context import (
    MarketDataGateway,
    RiskGateway,
    StrategyContext,
    StrategyIdentity,
    TimeProvider,
    TradeExporter,
    TradeHistoryRecorder,
)
from algo.strategy_engine.strategy_registry import StrategyRegistry, default_registry
from algo.strategy_engine.strategy_runner import StrategyRunner

if TYPE_CHECKING:
    from sqlalchemy.orm import Session, sessionmaker

    from algo.brokers.broker_base import BrokerBase
    from algo.common.enums import Exchange
    from algo.services.expiry_service import ExpiryService
    from algo.services.instrument_service import InstrumentService
    from algo.services.pricing_service import SpotPriceProvider

_DEFAULT_BASE_LOGGER = logging.getLogger("algo.strategy")


class InstanceFactory:
    """Builds ``StrategyRunner`` instances from configuration and shared
    dependencies."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        broker: BrokerBase,
        market_data: MarketDataGateway,
        risk: RiskGateway,
        time_provider: TimeProvider,
        instrument_service: InstrumentService | None = None,
        expiry_service: ExpiryService | None = None,
        spot_price_provider: SpotPriceProvider | None = None,
        trade_exporter: TradeExporter | None = None,
        trade_history_recorder: TradeHistoryRecorder | None = None,
        registry: StrategyRegistry | None = None,
        parameter_loader: ParameterLoader | None = None,
        base_logger: logging.Logger | None = None,
    ) -> None:
        """All cross-instance collaborators are injected here, once. ``registry``
        defaults to the process-wide ``default_registry``; ``parameter_loader``
        and ``base_logger`` to sensible shared defaults -- every one of them is
        overridable for tests. ``instrument_service``/``expiry_service``/
        ``spot_price_provider`` are optional, generic-engine-agnostic extras
        threaded through onto every built ``StrategyContext`` for strategies
        (like Strategy-1) that require them -- see StrategyContext's own
        docstring for why they live there rather than being a required part of
        this factory's signature."""
        self._session_factory = session_factory
        self._broker = broker
        self._market_data = market_data
        self._risk = risk
        self._time = time_provider
        self._instrument_service = instrument_service
        self._expiry_service = expiry_service
        self._spot_price_provider = spot_price_provider
        self._trade_exporter = trade_exporter
        self._trade_history_recorder = trade_history_recorder
        self._registry = registry if registry is not None else default_registry
        self._parameter_loader = (
            parameter_loader if parameter_loader is not None else ParameterLoader()
        )
        self._base_logger = base_logger if base_logger is not None else _DEFAULT_BASE_LOGGER

    def build_runner(
        self,
        *,
        strategy_id: str,
        instrument: str,
        account_id: int,
        exchange: Exchange,
    ) -> StrategyRunner:
        """Assemble and return a ready-to-``start()`` runner for one
        (strategy, instrument, account).

        Does not start the runner -- construction (which touches config and the
        database) is separated from starting (which runs recovery and begins
        dispatch), so a caller can build every instance, verify the whole set,
        and only then start them. Raises ``StrategyNotRegisteredError`` for an
        unknown strategy id, ``FileNotFoundError``/``ValidationError`` for a
        bad config, and propagates any database error from identity creation.
        """
        strategy_cls = self._registry.get(strategy_id)
        schema = strategy_cls.config_schema()
        config = self._parameter_loader.load_strategy_config(strategy_id, instrument, schema)

        instance_id = self._ensure_instance_row(
            strategy_id=strategy_id,
            instrument=instrument,
            account_id=account_id,
            exchange=exchange,
        )

        identity = StrategyIdentity(
            instance_id=instance_id,
            strategy_id=strategy_id,
            instrument=instrument,
            account_id=account_id,
            exchange=exchange,
        )
        logger = logging.LoggerAdapter(
            self._base_logger,
            {
                "strategy_id": strategy_id,
                "instrument": instrument,
                "account_id": account_id,
                "instance_id": instance_id,
            },
        )
        context = StrategyContext(
            identity=identity,
            config=config,
            session_factory=self._session_factory,
            broker=self._broker,
            market_data=self._market_data,
            risk=self._risk,
            time=self._time,
            logger=logger,
            instrument_service=self._instrument_service,
            expiry_service=self._expiry_service,
            spot_price_provider=self._spot_price_provider,
            trade_exporter=self._trade_exporter,
            trade_history_recorder=self._trade_history_recorder,
        )
        strategy = strategy_cls(context)
        return StrategyRunner(strategy)

    # -- Internal ------------------------------------------------------

    def _ensure_instance_row(
        self,
        *,
        strategy_id: str,
        instrument: str,
        account_id: int,
        exchange: Exchange,
    ) -> int:
        """Get-or-create the persistent ``StrategyInstance`` row and return its
        id, committing in its own short transaction so the identity is durable
        before any context is built around it."""
        with self._unit_of_work() as session:
            repo = StrategyInstanceRepository(session)
            instance, created = repo.get_or_create(
                strategy_id=strategy_id,
                instrument=instrument,
                account_id=account_id,
                exchange=exchange,
            )
            instance_id = instance.id
            if created:
                self._base_logger.info(
                    "created StrategyInstance row id=%d for %s/%s@acct%d",
                    instance_id,
                    strategy_id,
                    instrument,
                    account_id,
                )
        return instance_id

    @contextmanager
    def _unit_of_work(self) -> Iterator[Session]:
        """One committed transaction against the injected factory (see the
        matching note in ``strategy_runner``: not ``session_scope()``, because
        the factory here may be a test-injected one, not the global)."""
        with unit_of_work(self._session_factory) as session:
            yield session
