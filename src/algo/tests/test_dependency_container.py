"""Tests for DependencyContainer.

Uses the platform's real, committed config files (configs/app.yaml,
brokers.yaml, accounts.yaml, risk.yaml, market_data.yaml,
strategies/strategy_1/{nifty,sensex}.yaml) with DATABASE_URL pointed at
SQLite, plus fakes only for the four seams that have no concrete
implementation anywhere in this codebase yet (InstrumentService,
ExpiryService, SpotPriceProvider, TickStream). Everything else -- the broker,
market data, risk core, reconciliation engine, instance factory, scheduler --
is the real class.

``build_engine`` is monkeypatched for the lifecycle tests only: the real one
is hardcoded to Postgres-flavoured ``connect_args`` (``connect_timeout``,
``application_name``) that SQLite's driver rejects. Since ``create_engine()``
is lazy (no connection is opened until first use), construction-only tests
work fine against the real ``build_engine`` with a SQLite DATABASE_URL; only
tests that actually touch the database (``start()``) need the patched engine.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal

import pytest
from sqlalchemy import BigInteger, create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles

import algo.dependency_container as dependency_container_module
from algo.brokers.broker_base import InstrumentIdentifier
from algo.common.enums import Exchange
from algo.database.models import Base
from algo.dependency_container import DependencyContainer
from algo.market_data.market_data_service import MarketDataService


@compiles(BigInteger, "sqlite")
def _bigint_sqlite(type_, compiler, **kw):  # noqa: ANN001, ANN202
    return "INTEGER"


@compiles(JSONB, "sqlite")
def _jsonb_sqlite(type_, compiler, **kw):  # noqa: ANN001, ANN202
    return "JSON"


# ---------------------------------------------------------------------------
# Fakes for the four seams with no concrete implementation yet
# ---------------------------------------------------------------------------


class FakeInstrumentService:
    # Must cover every instrument in the committed app.yaml (the container builds
    # one runner per instance). underlying_symbol overrides where identity !=
    # broker-dump name (SENSEXBANK -> BANKEX).
    _SPECS = {
        "NIFTY": (Exchange.NFO, Decimal("50"), 75, Decimal("0.05"), None),
        "SENSEX": (Exchange.BFO, Decimal("100"), 20, Decimal("0.05"), None),
        "BANKNIFTY": (Exchange.NFO, Decimal("100"), 30, Decimal("0.05"), None),
        "FINNIFTY": (Exchange.NFO, Decimal("50"), 60, Decimal("0.05"), None),
        "MIDCPNIFTY": (Exchange.NFO, Decimal("25"), 120, Decimal("0.05"), None),
        "SENSEXBANK": (Exchange.BFO, Decimal("100"), 30, Decimal("0.05"), "BANKEX"),
    }

    def get_instrument_spec(self, instrument: str):
        from algo.services.instrument_service import InstrumentSpec

        exchange, strike_interval, lot_size, tick_size, underlying = self._SPECS[instrument]
        return InstrumentSpec(
            instrument=instrument, exchange=exchange, strike_interval=strike_interval,
            lot_size=lot_size, tick_size=tick_size, underlying_symbol=underlying,
        )


class FakeExpiryService:
    def get_current_weekly_expiry(self, instrument: str, as_of: date) -> date:
        return date(2026, 7, 9)


class FakeSpotPriceProvider:
    def get_spot_ltp(self, instrument: str) -> Decimal:
        return Decimal("25000")


class FakeTickStream:
    def is_connected(self) -> bool:
        return True

    def set_handlers(self, *, on_tick, on_connect, on_disconnect, on_reconnect) -> None:
        pass

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def subscribe(self, instruments: list[InstrumentIdentifier]) -> None:
        pass

    def unsubscribe(self, instruments: list[InstrumentIdentifier]) -> None:
        pass


def _seams() -> dict:
    return {
        "instrument_service": FakeInstrumentService(),
        "expiry_service": FakeExpiryService(),
        "spot_price_provider": FakeSpotPriceProvider(),
        "tick_stream": FakeTickStream(),
    }


@pytest.fixture(autouse=True)
def _sqlite_database_url(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite://")


@pytest.fixture
def patched_engine(monkeypatch, tmp_path):
    """For lifecycle tests: swap the real (Postgres-only) build_engine for a
    working SQLite one with all tables created.

    File-backed, not ``sqlite://`` in-memory: ``stop()`` calls
    ``engine.dispose()``, which is safe against a real Postgres database (data
    persists independently of the connection pool) but would destroy an
    in-memory SQLite database outright -- a file-backed DB is what actually
    matches production semantics for the restart tests below.
    """
    db_path = tmp_path / "container_test.db"

    def _fake_build_engine(settings):  # noqa: ARG001
        engine = create_engine(f"sqlite:///{db_path}")
        Base.metadata.create_all(engine)
        return engine

    monkeypatch.setattr(dependency_container_module, "build_engine", _fake_build_engine)


class TestConstruction:
    def test_builds_against_real_config_files(self):
        container = DependencyContainer(**_seams())

        assert container.app_config.environment == "development"
        assert len(container.app_config.instances) == 6
        assert container.is_started is False

    def test_wires_simulation_broker_by_default(self):
        from algo.brokers.rate_limiter import RateLimitedBroker
        from algo.brokers.simulation import SimulationBroker

        container = DependencyContainer(**_seams())

        assert isinstance(container.broker, RateLimitedBroker)
        assert isinstance(container.broker._inner, SimulationBroker)

    def test_wires_every_declared_service(self):
        from algo.risk.risk_core import RiskCore
        from algo.scheduler import PlatformScheduler
        from algo.services.reconciliation_engine import ReconciliationEngine
        from algo.strategy_engine.instance_factory import InstanceFactory

        container = DependencyContainer(**_seams())

        assert isinstance(container.market_data, MarketDataService)
        assert isinstance(container.risk_core, RiskCore)
        assert isinstance(container.reconciliation_engine, ReconciliationEngine)
        assert isinstance(container.instance_factory, InstanceFactory)
        assert isinstance(container.scheduler, PlatformScheduler)

    def test_log_level_applied_to_algo_logger(self):
        import logging

        DependencyContainer(**_seams())

        assert logging.getLogger("algo").level == logging.INFO

    def test_construction_does_not_touch_database(self, monkeypatch):
        """No query should happen before start() -- construction stays pure."""

        def _explode(*args, **kwargs):
            raise AssertionError("build_engine should not open a real connection during __init__")

        # build_engine itself is fine (lazy); what must NOT happen is any
        # session/connection use. We assert indirectly: constructing against
        # an intentionally-unreachable Postgres URL must still succeed.
        monkeypatch.setenv("DATABASE_URL", "postgresql://nouser:nopass@127.0.0.1:1/nodb")
        container = DependencyContainer(**_seams())
        assert container.engine is not None


class TestConfigValidation:
    def _write_configs(self, tmp_path, *, brokers_yaml: str | None = None, app_yaml: str | None = None):
        import shutil
        from pathlib import Path

        real_root = Path("configs")
        for name in ("app.yaml", "brokers.yaml", "accounts.yaml", "risk.yaml", "market_data.yaml"):
            shutil.copy(real_root / name, tmp_path / name)
        shutil.copytree(real_root / "strategies", tmp_path / "strategies")
        if brokers_yaml is not None:
            (tmp_path / "brokers.yaml").write_text(brokers_yaml, encoding="utf-8")
        if app_yaml is not None:
            (tmp_path / "app.yaml").write_text(app_yaml, encoding="utf-8")
        return tmp_path

    def test_missing_rate_limit_category_rejected(self, tmp_path):
        bad_brokers_yaml = """
active_broker: SIMULATION
rate_limits:
  ORDER_MUTATION: {max_calls: 10, per_seconds: 1.0}
simulation:
  synchronous: false
  initial_cash: "1000000"
  connection_failure_probability: 0.0
  rejection_probability: 0.0
  partial_fill_probability: 0.0
  ack_latency_seconds: 0.0
  fill_latency_seconds: 0.0
kite:
  api_key_env_var: "X"
  api_secret_env_var: "Y"
  access_token_env_var: "Z"
  read_retry_attempts: 3
  read_retry_delay_seconds: 0.5
  quote_batch_size: 200
"""
        root = self._write_configs(tmp_path, brokers_yaml=bad_brokers_yaml)

        with pytest.raises(Exception, match="missing categories"):
            DependencyContainer(config_root=root, **_seams())

    def test_unknown_account_reference_rejected(self, tmp_path):
        bad_app_yaml = """
environment: "development"
log_level: "INFO"
instances:
  - strategy_id: strategy_1
    instrument: nifty
    account: does_not_exist
"""
        root = self._write_configs(tmp_path, app_yaml=bad_app_yaml)

        with pytest.raises(ValueError, match="unknown account name"):
            DependencyContainer(config_root=root, **_seams())


class TestBrokerSelection:
    def test_kite_without_token_store_raises_at_construction(self, tmp_path):
        import shutil
        from pathlib import Path

        real_root = Path("configs")
        for name in ("app.yaml", "accounts.yaml", "risk.yaml", "market_data.yaml"):
            shutil.copy(real_root / name, tmp_path / name)
        shutil.copytree(real_root / "strategies", tmp_path / "strategies")
        (tmp_path / "brokers.yaml").write_text(
            (real_root / "brokers.yaml").read_text(encoding="utf-8").replace(
                "active_broker: SIMULATION", "active_broker: KITE"
            ),
            encoding="utf-8",
        )

        from algo.brokers.exceptions import BrokerAuthenticationError

        with pytest.raises(BrokerAuthenticationError, match="AccessTokenStore"):
            DependencyContainer(config_root=tmp_path, **_seams())


class TestLifecycle:
    def test_start_creates_accounts_and_runners_then_stop_is_clean(self, patched_engine):
        container = DependencyContainer(**_seams())

        container.start()
        try:
            assert container.is_started is True
            assert len(container.runners) == 6
            assert container._account_ids["primary"] > 0
        finally:
            container.stop()

        assert container.is_started is False

    def test_start_is_idempotent(self, patched_engine):
        container = DependencyContainer(**_seams())
        container.start()
        try:
            first_runner_ids = [id(r) for r in container.runners]
            container.start()
            assert [id(r) for r in container.runners] == first_runner_ids
        finally:
            container.stop()

    def test_stop_is_idempotent(self, patched_engine):
        container = DependencyContainer(**_seams())
        container.start()
        container.stop()
        container.stop()  # must not raise
        assert container.is_started is False

    def test_stop_before_start_is_noop(self, patched_engine):
        container = DependencyContainer(**_seams())
        container.stop()  # must not raise
        assert container.is_started is False

    def test_restart_resolves_same_account_row(self, patched_engine):
        container = DependencyContainer(**_seams())
        container.start()
        first_id = container._account_ids["primary"]
        container.stop()

        container.start()
        try:
            assert container._account_ids["primary"] == first_id
        finally:
            container.stop()

    def test_one_shutdown_step_failure_does_not_block_the_rest(self, patched_engine, monkeypatch):
        container = DependencyContainer(**_seams())
        container.start()

        def _explode():
            raise RuntimeError("boom")

        monkeypatch.setattr(container.market_data, "stop", _explode)

        container.stop()  # must not raise despite market_data.stop() failing

        assert container.is_started is False
