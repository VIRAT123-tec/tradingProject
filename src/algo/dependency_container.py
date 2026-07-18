"""Dependency injection container: the composition root that constructs and
wires every application service from validated configuration, so the rest of
the platform depends on abstractions (BrokerBase, RiskGateway,
MarketDataGateway, TimeProvider, ...) and never on how a concrete instance was
built.

Dependency direction (resolving the module's original TODO): configuration and
the database come first; the broker depends on nothing else; market data,
risk, and reconciliation each depend on the broker + database + time; the
strategy engine (InstanceFactory) depends on all of the above; the scheduler
depends only on the strategy runners the strategy engine produces. Nothing
later in this list is depended on by anything earlier -- see
"explain the dependency graph" in this task's summary for the full picture.

Two-phase lifecycle, deliberately mirroring patterns already established
elsewhere in this codebase (``StrategyRunner``'s CREATED->RUNNING split,
``InstanceFactory.build_runner`` separating construction from starting):
``__init__`` performs pure construction -- load and validate every config file
(fail fast on a bad one), build the database engine/session factory, build the
broker and every service around it. No I/O against the broker or a strategy
side-effect happens here. ``start()`` is where actual startup I/O happens:
broker authentication, resolving/creating configured accounts, building and
registering strategy instances (which triggers each strategy's own restart
recovery), running startup reconciliation, connecting market data, and
starting the scheduler. ``stop()`` reverses it for graceful shutdown.

No module-level global state: every constructed object is an instance
attribute on the container, built via the *pure* ``build_engine``/
``build_session_factory`` functions (never the process-wide
``get_engine()``/``get_session_factory()`` singletons those same modules also
expose) specifically so an independent container -- with its own engine, its
own everything -- can be constructed in a test with no shared state between
instances.

Honest about what still needs to be supplied from outside: ``instrument_service``,
``expiry_service``, ``spot_price_provider``, and ``tick_stream`` have no
concrete implementation anywhere in this codebase yet (each is still a
Protocol-only seam from earlier tasks -- building real ones needs either
per-instrument config data that does not exist yet, or a broker-specific
adapter not yet built). Rather than fabricate a fake "real" implementation,
this container requires them as constructor parameters: a missing one is a
constructor-time ``TypeError``, not a runtime surprise. See this task's
"assumptions" summary for the complete list and why each is out of scope here.
"""

from __future__ import annotations

import logging
import os
import random
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field, model_validator

import algo.strategy_engine.strategies.strategy_1.strategy  # noqa: F401 -- side effect: registers "strategy_1"
from algo.brokers.broker_base import BrokerBase
from algo.brokers.exceptions import BrokerAuthenticationError, BrokerConnectionError, BrokerError
from algo.brokers.rate_limiter import RateLimitCategory, RateLimitConfig, RateLimitedBroker, RateLimitRule
from algo.brokers.simulation import InstrumentCatalog, SimulationBroker, SimulationConfig, StaticPriceSource
from algo.common.enums import BrokerName
from algo.database.database import build_engine, load_database_settings
from algo.database.repositories.account_repository import AccountRepository
from algo.database.session import build_session_factory
from algo.market_data import MarketDataConfig, MarketDataService
from algo.reporting.trade_report import TradeReportExporter
from algo.risk.kill_switch import KillSwitch
from algo.risk.risk_core import RiskCore, RiskCoreConfig
from algo.scheduler import PlatformScheduler, SchedulerConfig
from algo.services.live_seams import BrokerSpotPriceProvider
from algo.services.order_update_processor import OrderUpdateProcessor
from algo.services.reconciliation_engine import ReconciliationEngine
from algo.services.time_service import SystemTimeProvider
from algo.services.trade_history_recorder import TradeHistoryRecorder
from algo.strategy_engine.instance_factory import InstanceFactory
from algo.strategy_engine.parameter_loader import ParameterLoader
from algo.strategy_engine.strategy_registry import StrategyRegistry
from algo.strategy_engine.strategy_scheduler import MonitoringScheduler, MonitoringSchedulerConfig

if TYPE_CHECKING:
    from sqlalchemy import Engine
    from sqlalchemy.orm import Session, sessionmaker

    from algo.brokers.kite.kite_auth import AccessTokenStore
    from algo.market_data.websocket_manager import TickStream
    from algo.services.expiry_service import ExpiryService
    from algo.services.instrument_service import InstrumentService
    from algo.services.pricing_service import SpotPriceProvider
    from algo.services.reconciliation_engine import PositionReconciler
    from algo.strategy_engine.strategy_context import TimeProvider
    from algo.strategy_engine.strategy_runner import StrategyRunner


# ---------------------------------------------------------------------------
# Configuration schemas -- one per config file this container loads
# ---------------------------------------------------------------------------


class StrategyInstanceSpec(BaseModel):
    """One (strategy, instrument, account) combination to run, declared in
    ``configs/app.yaml``."""

    model_config = ConfigDict(frozen=True)

    strategy_id: str
    instrument: str
    account: str = Field(description="References a name in AccountsConfig.accounts.")


class AppConfig(BaseModel):
    """Top-level platform settings (``configs/app.yaml``): which strategy
    instances to run, and how verbosely to log. Deliberately does not
    duplicate anything more specific -- broker selection lives in
    ``brokers.yaml``, risk limits in ``risk.yaml``, and so on."""

    model_config = ConfigDict(frozen=True)

    environment: str = Field(description="e.g. 'development', 'paper', 'live' -- for logging/alerting context.")
    log_level: str = Field(description="Root 'algo' logger level, e.g. 'INFO', 'DEBUG'.")
    instances: list[StrategyInstanceSpec] = Field(min_length=1)
    report_output_dir: str | None = Field(
        default=None,
        description=(
            "Directory for the per-day trade report .xlsx files (one file per "
            "trading day, one row per option leg). Relative paths resolve "
            "against the process working directory. Unset (the default) "
            "disables trade-report export entirely -- nothing is written."
        ),
    )


class SimulationBrokerSettings(BaseModel):
    """Simulation-broker tuning, mirroring ``SimulationConfig`` -- kept as its
    own schema (not reusing SimulationConfig directly) so brokers.yaml is not
    coupled to that class's internal field set."""

    model_config = ConfigDict(frozen=True)

    synchronous: bool
    initial_cash: Decimal
    connection_failure_probability: float = Field(ge=0, le=1)
    rejection_probability: float = Field(ge=0, le=1)
    partial_fill_probability: float = Field(ge=0, le=1)
    ack_latency_seconds: float = Field(ge=0)
    fill_latency_seconds: float = Field(ge=0)


class KiteBrokerSettings(BaseModel):
    """Kite-broker tuning plus *names* of the environment variables holding
    credentials -- never the credentials themselves, per the platform's
    standing "secrets never in YAML" rule."""

    model_config = ConfigDict(frozen=True)

    api_key_env_var: str
    api_secret_env_var: str
    access_token_env_var: str
    read_retry_attempts: int = Field(gt=0)
    read_retry_delay_seconds: float = Field(ge=0)
    quote_batch_size: int = Field(gt=0)
    request_timeout_seconds: float = Field(
        default=7.0, gt=0,
        description="Socket timeout applied to the KiteConnect client, bounding "
        "EVERY Kite HTTP call -- including mutating place/modify/cancel, which the "
        "SDK offers no per-call timeout for. Without it those calls are unbounded; "
        "with it, a hung request surfaces as a (mutation-ambiguous) timeout error "
        "the platform already handles, rather than blocking a strategy indefinitely.",
    )


class RateLimitRuleSpec(BaseModel):
    model_config = ConfigDict(frozen=True)

    max_calls: int = Field(gt=0)
    per_seconds: float = Field(gt=0)


class BrokersConfig(BaseModel):
    """Broker selection and tuning (``configs/brokers.yaml``). Both
    ``simulation`` and ``kite`` blocks are always required, regardless of
    which is active -- switching brokers is then a one-line
    ``active_broker`` edit, never a YAML restructuring."""

    model_config = ConfigDict(frozen=True)

    active_broker: BrokerName
    rate_limits: dict[RateLimitCategory, RateLimitRuleSpec]
    simulation: SimulationBrokerSettings
    kite: KiteBrokerSettings

    @model_validator(mode="after")
    def _check_all_rate_limit_categories_present(self) -> "BrokersConfig":
        missing = set(RateLimitCategory) - set(self.rate_limits)
        if missing:
            raise ValueError(
                "brokers.yaml rate_limits is missing categories "
                f"{sorted(m.value for m in missing)} -- RateLimitedBroker treats "
                "an absent category as unthrottled, so every category must be "
                "configured explicitly, not left to that fallback"
            )
        return self


class AccountSpec(BaseModel):
    """One trading account (``configs/accounts.yaml``)."""

    model_config = ConfigDict(frozen=True)

    name: str = Field(description="Key referenced by app.yaml's instances[].account.")
    broker: BrokerName
    broker_client_id: str | None = Field(
        description="Broker's own client id. Explicit null for SIMULATION accounts, "
        "which have no real broker-side identity."
    )
    display_name: str


class AccountsConfig(BaseModel):
    """All configured trading accounts (``configs/accounts.yaml``)."""

    model_config = ConfigDict(frozen=True)

    accounts: list[AccountSpec] = Field(min_length=1)

    @model_validator(mode="after")
    def _check_unique_names(self) -> "AccountsConfig":
        names = [a.name for a in self.accounts]
        if len(names) != len(set(names)):
            raise ValueError(f"account names must be unique, got {names}")
        return self


# ---------------------------------------------------------------------------
# The container
# ---------------------------------------------------------------------------


class DependencyContainer:
    """Composition root: builds and holds every application service as an
    instance attribute. See the module docstring for the full design.
    """

    def __init__(
        self,
        *,
        instrument_service: InstrumentService,
        expiry_service: ExpiryService,
        tick_stream: TickStream,
        spot_price_provider: SpotPriceProvider | None = None,
        config_root: Path | None = None,
        position_reconciler: PositionReconciler | None = None,
        access_token_store: AccessTokenStore | None = None,
        strategy_registry: StrategyRegistry | None = None,
        simulation_instrument_catalog: InstrumentCatalog | None = None,
        simulation_price_source: StaticPriceSource | None = None,
        time_provider: TimeProvider | None = None,
        scheduler_config: SchedulerConfig | None = None,
        monitoring_scheduler_config: MonitoringSchedulerConfig | None = None,
        broker: BrokerBase | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        """Load and validate every config file, then construct every service.
        No broker/database I/O happens here -- see ``start()``. Raises
        ``pydantic.ValidationError`` immediately on any invalid config file, and
        ``FileNotFoundError`` if one is missing -- both deliberately fatal at
        construction, not discovered later at market open.

        ``instrument_service``/``expiry_service``/``spot_price_provider``/
        ``tick_stream`` are required: no concrete implementation of any of
        them exists yet elsewhere in this codebase (see the module docstring).
        ``position_reconciler`` and ``access_token_store`` are optional --
        the former degrades safely to a flagged break instead of an automatic
        repair when absent (``ReconciliationEngine``'s own design), and the
        latter is only needed when ``brokers.yaml`` selects Kite.
        ``simulation_instrument_catalog``/``simulation_price_source`` let a
        caller (a test, or a future seeding step) hand the simulation broker
        real contracts/prices; unset, it starts empty (structurally valid,
        but unable to resolve or price any instrument until seeded).
        ``time_provider`` defaults to ``SystemTimeProvider()`` (real wall
        clock) for every production entrypoint; a test overrides it with a
        controllable clock so entry/cutoff trigger timing -- otherwise pinned
        to real IST wall-clock time -- can be driven deterministically.
        ``scheduler_config`` defaults to ``SchedulerConfig()`` (1-second
        background poll); a test driving triggers manually via
        ``runner.dispatch_time_trigger()`` against a shared, mutable
        ``time_provider`` should pass a very long ``poll_interval_seconds``
        here, otherwise the real background thread started by
        ``scheduler.start()`` races the test's own manual dispatch calls --
        both would independently see the same trigger as due against the
        same clock and could dispatch it concurrently.
        ``broker`` overrides the broker this container would otherwise build
        from ``brokers.yaml`` (bypassing ``_build_broker()`` entirely,
        including the rate limiter -- pass an already-``RateLimitedBroker``-
        wrapped instance if that matters to the caller). A real broker
        (Kite, or the exchange it talks to) has its own state that persists
        independently of this process restarting; ``SimulationBroker``'s does
        not (it is purely in-memory), so a test simulating a restart against
        the *same* broker-side state -- e.g. to exercise recovery of an
        already-OPEN position without reconciliation seeing a
        broker/database mismatch that only exists because a *second*,
        freshly-built ``SimulationBroker`` was never told about the first
        one's fills -- passes the first container's own ``.broker`` here.
        """
        self._logger = logger if logger is not None else logging.getLogger("algo.container")
        self._access_token_store = access_token_store
        self._started = False
        self._account_ids: dict[str, int] = {}
        self._runners: list[StrategyRunner] = []

        self._loader = ParameterLoader(config_root)

        # -- Configuration: loaded and validated up front (fail fast) -----
        self.app_config = self._loader.load(self._loader.config_root / "app.yaml", AppConfig)
        self.brokers_config = self._loader.load(self._loader.config_root / "brokers.yaml", BrokersConfig)
        self.accounts_config = self._loader.load(self._loader.config_root / "accounts.yaml", AccountsConfig)
        self.risk_config = self._loader.load(self._loader.config_root / "risk.yaml", RiskCoreConfig)
        self.market_data_config = self._loader.load(
            self._loader.config_root / "market_data.yaml", MarketDataConfig
        )
        self._check_account_references()
        self._configure_logging()

        # -- Database -------------------------------------------------------
        self.database_settings = load_database_settings()
        self.engine: Engine = build_engine(self.database_settings)
        self.session_factory: sessionmaker[Session] = build_session_factory(self.engine)

        # -- Time -------------------------------------------------------------
        self.time_provider: TimeProvider = time_provider if time_provider is not None else SystemTimeProvider()

        # -- Broker (Simulation or Kite, per brokers.yaml) -------------------
        self.broker: BrokerBase = broker if broker is not None else self._build_broker(
            simulation_instrument_catalog or InstrumentCatalog(),
            simulation_price_source or StaticPriceSource({}),
        )

        # -- Market data ------------------------------------------------------
        self.market_data = MarketDataService(
            tick_stream=tick_stream,
            poller=self.broker,
            time_provider=self.time_provider,
            config=self.market_data_config,
        )

        # -- Risk core --------------------------------------------------------
        self.risk_core = RiskCore(
            config=self.risk_config,
            broker=self.broker,
            session_factory=self.session_factory,
            time_provider=self.time_provider,
        )

        # -- Kill switch (producer for the flags risk_core/monitor already read)
        self.kill_switch = KillSwitch(
            session_factory=self.session_factory, time_provider=self.time_provider
        )

        # -- Reconciliation engine --------------------------------------------
        self.reconciliation_engine = ReconciliationEngine(
            broker=self.broker,
            session_factory=self.session_factory,
            time_provider=self.time_provider,
            position_reconciler=position_reconciler,
        )

        # -- Order-update processor (idempotent push-fill application) --------
        self.order_update_processor = OrderUpdateProcessor(
            session_factory=self.session_factory, time_provider=self.time_provider
        )

        # -- Spot price provider ----------------------------------------------
        # Needs the broker (to read the cash-segment spot LTP), which this
        # container owns -- so when a caller does not inject one, the container
        # builds the broker-backed default itself from the instrument service's
        # spot reference. This resolves the natural chicken-and-egg (the spot
        # provider depends on the very broker the container constructs).
        self._spot_price_provider: SpotPriceProvider = (
            spot_price_provider
            if spot_price_provider is not None
            else BrokerSpotPriceProvider(broker=self.broker, instrument_service=instrument_service)
        )

        # -- Trade report exporter (optional post-close side channel) --------
        # Only built when app.yaml sets report_output_dir; otherwise export is
        # disabled and every StrategyContext carries trade_exporter=None. The
        # exit path calls it after a position closes, always behind a guard, so
        # reporting can never affect trading.
        self.trade_exporter: TradeReportExporter | None = (
            TradeReportExporter(
                session_factory=self.session_factory,
                output_dir=Path(self.app_config.report_output_dir),
            )
            if self.app_config.report_output_dir is not None
            else None
        )

        # -- Trade-history recorder (permanent analytics store) --------------
        # Always on (no config gate): the database is the permanent historical
        # record, so every completed trade is recorded regardless of settings.
        # "mode" is the active broker (SIMULATION -> PAPER, KITE -> LIVE), the
        # same signal start_paper/start_live enforce. Like the exporter, the
        # exit path calls it behind a guard, so recording can never affect
        # trading; a failed write leaves the trade intact in the execution
        # tables (backfillable).
        self.trade_history_recorder = TradeHistoryRecorder(
            session_factory=self.session_factory,
            mode="PAPER" if self.broker.broker_name is BrokerName.SIMULATION else "LIVE",
        )

        # -- Strategy engine ----------------------------------------------------
        self._instrument_service = instrument_service
        self.instance_factory = InstanceFactory(
            session_factory=self.session_factory,
            broker=self.broker,
            market_data=self.market_data,
            risk=self.risk_core,
            time_provider=self.time_provider,
            instrument_service=instrument_service,
            expiry_service=expiry_service,
            spot_price_provider=self._spot_price_provider,
            trade_exporter=self.trade_exporter,
            trade_history_recorder=self.trade_history_recorder,
            registry=strategy_registry,
        )

        # -- Scheduler ------------------------------------------------------
        self.scheduler = PlatformScheduler(
            time_provider=self.time_provider, config=scheduler_config
        )

        # -- Monitoring scheduler (intraday stop-loss/target heartbeat) ------
        # Drives each open position's exit evaluation on a cadence, independent
        # of the live tick feed. See strategy_scheduler.py; resolves the "no
        # intraday exit evaluation" gap.
        self.monitoring_scheduler = MonitoringScheduler(config=monitoring_scheduler_config)

    # -- Lifecycle -------------------------------------------------------

    def start(self) -> None:
        """Bring the platform up. Idempotent: a second call is a no-op.

        Order (see this task's summary for the full rationale): authenticate
        the broker -> resolve/create every configured account -> build a
        StrategyRunner for every declared instance (construction only, per
        InstanceFactory's own contract -- does not start it or touch the
        broker) -> run startup reconciliation (repairs any crashed in-doubt
        database state against broker truth *before* any strategy reads it)
        -> connect market data -> register every runner with the scheduler
        (this is what actually starts each one, triggering its own
        ``Strategy.recover()``), the monitoring scheduler (intraday exit
        heartbeat), and the market-data tick feed -> start both scheduler loops.
        """
        if self._started:
            return
        self._logger.info("starting platform (environment=%s)", self.app_config.environment)

        # H4: authenticate and then verify broker connectivity BEFORE any
        # strategy is built or armed, and fail fast on either. A platform that
        # cannot reach its broker must not start scheduling entries -- better a
        # loud refusal at 09:00 than a silent no-op (or worse) at 09:20.
        self.broker.authenticate()
        health = self.broker.health_check()
        if not health.healthy:
            raise BrokerConnectionError(
                f"broker connectivity check failed at startup: {health.detail or 'unhealthy'}"
            )
        self._logger.info(
            "broker connectivity verified (%s, latency=%.0fms)",
            self.broker.broker_name.value, health.latency_ms or 0.0,
        )

        self._account_ids = self._ensure_accounts()

        self._runners = [
            self.instance_factory.build_runner(
                strategy_id=spec.strategy_id,
                instrument=spec.instrument,
                account_id=self._account_ids[spec.account],
                exchange=self._instrument_service.get_instrument_spec(spec.instrument).exchange,
            )
            for spec in self.app_config.instances
        ]

        report = self.reconciliation_engine.reconcile()
        if report.broker_unavailable:
            self._logger.critical(
                "startup reconciliation could not reach the broker; proceeding, "
                "but in-doubt state from any prior crash remains unresolved"
            )
        else:
            self._logger.info(
                "startup reconciliation: %d subject(s) examined, %d break(s) recorded",
                len(report.items), report.breaks_recorded,
            )

        # H3: connect the broker's order-update feed and route updates through
        # the idempotent processor. The callback is registered *before* the
        # connection opens so no early update is missed. A websocket failure
        # degrades gracefully -- fills are still detected by the polling path.
        try:
            self.broker.register_order_update_callback(self.order_update_processor.process)
            self.broker.connect_websocket()
            self._logger.info("order-update websocket connected")
        except BrokerError:
            self._logger.error(
                "order-update websocket unavailable at startup; fills will rely on polling",
                exc_info=True,
            )

        self.market_data.start()

        for runner in self._runners:
            self.scheduler.register(runner)  # starts the runner -> Strategy.recover()
            self.monitoring_scheduler.register(runner)  # intraday exit heartbeat (pull path)
            # Push path: deliver every live tick to this runner. The runner's
            # monitor ignores ticks for instruments it is not holding, so a
            # simple broadcast is correct. This activates automatically once a
            # concrete live TickStream is supplied; until then the monitoring
            # scheduler's polling is the working path.
            self.market_data.register_consumer(runner.dispatch_tick)

        self.scheduler.start()
        self.monitoring_scheduler.start()

        self._started = True
        self._logger.info("platform started with %d strategy instance(s)", len(self._runners))

    def stop(self) -> None:
        """Graceful shutdown, reversing ``start()``: stop the scheduler
        (which stops every registered runner, running each strategy's own
        ``on_shutdown()``), stop market data, close the broker, and dispose the
        database connection pool. Idempotent. Each step is isolated -- one
        step's failure is logged but never prevents the remaining cleanup from
        running.
        """
        if not self._started:
            return
        self._logger.info("stopping platform")

        for name, step in (
            ("monitoring scheduler", self.monitoring_scheduler.stop),
            ("scheduler", self.scheduler.stop),
            ("market_data", self.market_data.stop),
            ("broker", self.broker.close),
            ("database engine", self.engine.dispose),
        ):
            try:
                step()
            except Exception:  # noqa: BLE001 -- one step's failure must not block the rest
                self._logger.error("error stopping %s during shutdown", name, exc_info=True)

        self._started = False
        self._logger.info("platform stopped")

    @property
    def is_started(self) -> bool:
        return self._started

    @property
    def runners(self) -> list[StrategyRunner]:
        """Every strategy runner built by the last ``start()`` call."""
        return list(self._runners)

    # -- Internal ----------------------------------------------------------

    def _check_account_references(self) -> None:
        """app.yaml's instances[].account and accounts.yaml's accounts[].name
        are validated independently (they live in separate files, so neither
        Pydantic model can see the other) -- cross-check them here so a typo'd
        account reference fails fast at construction with a clear message,
        rather than as a confusing KeyError deep inside start()."""
        known = {a.name for a in self.accounts_config.accounts}
        unknown = {spec.account for spec in self.app_config.instances} - known
        if unknown:
            raise ValueError(
                f"app.yaml instances reference unknown account name(s) {sorted(unknown)}; "
                f"accounts.yaml declares {sorted(known)}"
            )

    def _configure_logging(self) -> None:
        """Apply app.yaml's log_level to the shared 'algo' logger namespace --
        every module in this platform logs under an 'algo.*' name, so setting
        the level on the 'algo' root propagates to all of them."""
        logging.getLogger("algo").setLevel(self.app_config.log_level)

    def _build_broker(
        self, simulation_instrument_catalog: InstrumentCatalog, simulation_price_source: StaticPriceSource
    ) -> BrokerBase:
        config = self.brokers_config
        if config.active_broker is BrokerName.SIMULATION:
            inner: BrokerBase = SimulationBroker(
                instrument_catalog=simulation_instrument_catalog,
                price_source=simulation_price_source,
                config=SimulationConfig(
                    synchronous=config.simulation.synchronous,
                    initial_cash=config.simulation.initial_cash,
                    connection_failure_probability=config.simulation.connection_failure_probability,
                    rejection_probability=config.simulation.rejection_probability,
                    partial_fill_probability=config.simulation.partial_fill_probability,
                    ack_latency_seconds=config.simulation.ack_latency_seconds,
                    fill_latency_seconds=config.simulation.fill_latency_seconds,
                ),
                rng=random.Random(),
            )
        elif config.active_broker is BrokerName.KITE:
            inner = self._build_kite_broker(config.kite)
        else:  # pragma: no cover -- BrokerName is exhaustively KITE|SIMULATION
            raise ValueError(f"unknown active_broker {config.active_broker!r}")

        rate_limit_config = RateLimitConfig(
            rules={
                category: RateLimitRule(max_calls=rule.max_calls, per_seconds=rule.per_seconds)
                for category, rule in config.rate_limits.items()
            }
        )
        return RateLimitedBroker(inner, rate_limit_config)

    def _build_kite_broker(self, kite_config: KiteBrokerSettings) -> BrokerBase:
        # Imported lazily: only needed on this branch, and keeps a Kite SDK
        # import from being unconditionally required just to run in simulation.
        from kiteconnect import KiteConnect

        from algo.brokers.kite.kite_auth import KiteSession
        from algo.brokers.kite.kite_broker import KiteBroker, KiteBrokerConfig
        from algo.brokers.kite.websocket import KiteOrderUpdateStream

        if self._access_token_store is None:
            raise BrokerAuthenticationError(
                "brokers.yaml selects active_broker=KITE but no AccessTokenStore "
                "was injected into DependencyContainer"
            )
        api_key = _require_env(kite_config.api_key_env_var)
        api_secret = _require_env(kite_config.api_secret_env_var)

        # M2: bound every Kite HTTP call (including mutating place/modify/cancel,
        # which the SDK has no per-call timeout for) with a client-level socket
        # timeout, so a hung request can never block a strategy indefinitely.
        client = KiteConnect(api_key=api_key, timeout=kite_config.request_timeout_seconds)
        session = KiteSession(client=client, api_secret=api_secret, token_store=self._access_token_store)

        def ticker_factory():
            from kiteconnect import KiteTicker

            token = self._access_token_store.get_access_token()
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

    def _ensure_accounts(self) -> dict[str, int]:
        """Resolve every configured account name to a durable Account row id,
        creating rows that do not exist yet (idempotent -- a restart finds the
        same rows)."""
        ids: dict[str, int] = {}
        session = self.session_factory()
        try:
            repo = AccountRepository(session)
            for spec in self.accounts_config.accounts:
                account, created = repo.get_or_create(
                    broker=spec.broker,
                    broker_client_id=spec.broker_client_id,
                    display_name=spec.display_name,
                )
                ids[spec.name] = account.id
                if created:
                    self._logger.info(
                        "created Account row id=%d for %r (%s)", account.id, spec.name, spec.broker.value
                    )
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
        return ids


def _require_env(var_name: str) -> str:
    value = os.environ.get(var_name)
    if not value:
        raise BrokerAuthenticationError(
            f"required environment variable {var_name!r} is not set"
        )
    return value
