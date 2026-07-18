"""Shared fixtures for the integration/end-to-end test suite.

These tests deliberately exercise REAL collaborators wired together -- a real
``DependencyContainer``, real ``SimulationBroker``, real ``Strategy1`` (and
therefore real ``EntryLogic``/``ExitLogic``/``PositionMonitor``/
``PositionStateMachine``/``StrikeSelector``/``CombinedPremiumTracker``), real
``RiskCore``, real ``ReconciliationEngine``, real ``PlatformScheduler`` --
rather than the per-module fakes each module's own unit test suite uses. That
isolation is exactly right for a unit test and exactly wrong here: this
suite's entire purpose is catching bugs that only show up when real
components are wired together and can disagree about a contract in a way no
single-module test can see (a casing mismatch between two config files, a
param name drift between two modules, a Protocol satisfied in form but not in
the way its real caller actually uses it -- Task 26's own review already
found and fixed one exactly this shape: ``configs/app.yaml`` declared
instrument identities in a different case than ``risk.yaml``'s lookup keys,
which no existing unit test could have caught because none of them drove a
real entry through the real, committed config files).

Unit-level edge cases already covered by each module's own test suite (Tasks
11-23 -- e.g. every ``evaluate_exit`` priority-ordering permutation, every
``PositionStateMachine`` transition-legality case, every risk-check rejection
reason) are not re-derived here; this suite checks that the real wiring
between modules works, not that already-tested pure functions are correct.

The only fakes used anywhere in this suite are for the four seams that
genuinely have no concrete implementation anywhere in this codebase yet --
``InstrumentService``, ``ExpiryService``, ``SpotPriceProvider``, ``TickStream``
-- the same, already-documented gap from ``dependency_container.py``'s own
module docstring (Task 24) and ``start_paper.py``/``start_live.py``'s
``build_seams()`` (Task 25).

Determinism (an explicit Task 26 requirement):

* No real sleep, no real wall-clock dependency: every container in this suite
  is built with ``time_provider=MutableClock(...)``, fully controllable, and
  ``SimulationConfig(synchronous=True)`` (resolves every order fully inline,
  no background matching thread, no latency to wait out -- see
  ``brokers/simulation/config.py``'s own docstring for why this mode exists).
* No real network: nothing in this suite talks to the internet; the one
  Kite-broker integration test mocks the ``kiteconnect`` SDK at the process
  boundary (see ``test_kite_broker_wiring.py``).
* No flaky option-chain math: spot LTP, strike interval, and expiry are fixed
  values chosen so the ATM strike is an exact, non-rounded number.
* No real websocket: ``FakeTickStream`` never pushes a tick, which
  deliberately forces every price read through ``MarketDataService``'s
  polling fallback -- itself backed by the real, synchronous
  ``SimulationBroker`` -- so "the price the test just set" is what a poll
  actually observes, with no timing window to race.

File-backed (not in-memory) SQLite is used throughout, for the same reason
``test_dependency_container.py`` uses it: ``DependencyContainer.stop()`` calls
``engine.dispose()``, which destroys an in-memory SQLite database outright but
is a no-op against a real, persistent database (Postgres in production) --
file-backed SQLite is what actually matches that semantic, and is required for
the restart/crash-recovery tests, which construct a second, independent
container against the same on-disk state to simulate a real process restart.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import BigInteger, create_engine, event
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles

import algo.dependency_container as dependency_container_module
from algo.brokers.broker_base import InstrumentIdentifier
from algo.brokers.simulation import InstrumentCatalog, StaticPriceSource
from algo.common.enums import Exchange, OptionType
from algo.database.models import Base
from algo.dependency_container import DependencyContainer
from algo.scheduler import SchedulerConfig
from algo.strategy_engine.strategy_scheduler import MonitoringSchedulerConfig
from algo.services.instrument_service import InstrumentSpec


@pytest.fixture(autouse=True)
def _database_url_env(monkeypatch):
    """DependencyContainer.__init__ requires DATABASE_URL to be set (fails
    fast otherwise, by design -- see database.py). The value is never
    actually connected to: build_container() below always monkeypatches
    build_engine to point at a file-backed SQLite path instead."""
    monkeypatch.setenv("DATABASE_URL", "sqlite:///unused")


@compiles(BigInteger, "sqlite")
def _bigint_sqlite(type_, compiler, **kw):  # noqa: ANN001, ANN202
    return "INTEGER"


@compiles(JSONB, "sqlite")
def _jsonb_sqlite(type_, compiler, **kw):  # noqa: ANN001, ANN202
    return "JSON"


# ---------------------------------------------------------------------------
# Fixed, deterministic NIFTY option-chain scenario shared by most tests
# ---------------------------------------------------------------------------

NIFTY = "NIFTY"
NIFTY_EXPIRY = date(2026, 7, 9)  # a Thursday
NIFTY_SPOT = Decimal("25000")
NIFTY_STRIKE_INTERVAL = Decimal("50")
NIFTY_ATM_STRIKE = Decimal("25000")  # 25000 / 50 is exact -- no rounding ambiguity
NIFTY_LOT_SIZE = 75
NIFTY_TICK_SIZE = Decimal("0.05")

SENSEX = "SENSEX"
SENSEX_EXPIRY = date(2026, 7, 9)
SENSEX_SPOT = Decimal("81000")
SENSEX_STRIKE_INTERVAL = Decimal("100")
SENSEX_ATM_STRIKE = Decimal("81000")
SENSEX_LOT_SIZE = 20
SENSEX_TICK_SIZE = Decimal("0.05")

_SPECS: dict[str, InstrumentSpec] = {
    NIFTY: InstrumentSpec(
        instrument=NIFTY, exchange=Exchange.NFO, strike_interval=NIFTY_STRIKE_INTERVAL,
        lot_size=NIFTY_LOT_SIZE, tick_size=NIFTY_TICK_SIZE,
    ),
    SENSEX: InstrumentSpec(
        instrument=SENSEX, exchange=Exchange.BFO, strike_interval=SENSEX_STRIKE_INTERVAL,
        lot_size=SENSEX_LOT_SIZE, tick_size=SENSEX_TICK_SIZE,
    ),
}
_SPOT: dict[str, Decimal] = {NIFTY: NIFTY_SPOT, SENSEX: SENSEX_SPOT}
_EXPIRY: dict[str, date] = {NIFTY: NIFTY_EXPIRY, SENSEX: SENSEX_EXPIRY}


# ---------------------------------------------------------------------------
# Fakes for the four seams with no concrete implementation anywhere yet
# ---------------------------------------------------------------------------


class FakeInstrumentService:
    def get_instrument_spec(self, instrument: str) -> InstrumentSpec:
        return _SPECS[instrument]


class FakeExpiryService:
    def get_current_weekly_expiry(self, instrument: str, as_of: date) -> date:  # noqa: ARG002
        return _EXPIRY[instrument]


class FakeSpotPriceProvider:
    def get_spot_ltp(self, instrument: str) -> Decimal:
        return _SPOT[instrument]


class FakeTickStream:
    """Never pushes a tick -- see the module docstring for why that is
    deliberate: it forces every price read through the polling fallback."""

    def is_connected(self) -> bool:
        return True

    def set_handlers(self, *, on_tick, on_connect, on_disconnect, on_reconnect) -> None:  # noqa: ANN001
        pass

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def subscribe(self, instruments: list[InstrumentIdentifier]) -> None:
        pass

    def unsubscribe(self, instruments: list[InstrumentIdentifier]) -> None:
        pass


# ---------------------------------------------------------------------------
# Controllable clock
# ---------------------------------------------------------------------------


@dataclass
class MutableClock:
    """Fully controllable ``TimeProvider``. The IST wall-clock value is set
    directly on ``.ist`` -- this codebase's established test convention (see
    e.g. ``test_platform_scheduler.py``'s ``MutableTime``): only
    ``.time()``/``.date()`` are ever read from ``now_ist()``, so ``tzinfo`` is
    never semantically converted, and an arbitrary placeholder is fine.
    """

    ist: datetime

    def now(self) -> datetime:
        return self.ist

    def now_ist(self) -> datetime:
        return self.ist

    def today(self) -> date:
        return self.ist.date()

    def set_time(self, *, hour: int, minute: int, second: int = 0) -> None:
        self.ist = self.ist.replace(hour=hour, minute=minute, second=second)

    def advance(self, *, days: int = 0, seconds: float = 0) -> None:
        self.ist = self.ist + timedelta(days=days, seconds=seconds)


def make_clock(*, hour: int = 9, minute: int = 0, day: date = date(2026, 7, 8)) -> MutableClock:
    """A clock pinned to a fixed date/time. 2026-07-08 is a Wednesday --
    a genuine NIFTY weekly-expiry trading day, though ``FakeExpiryService``
    ignores the real calendar entirely and always answers ``NIFTY_EXPIRY``."""
    return MutableClock(datetime(day.year, day.month, day.day, hour, minute, tzinfo=timezone.utc))


# ---------------------------------------------------------------------------
# Config root + container construction
# ---------------------------------------------------------------------------


def make_config_root(
    tmp_path: Path,
    *,
    instrument: str = NIFTY,
    account: str = "primary",
    report_output_dir: str | None = None,
) -> Path:
    """A temp config root copying every real, committed config file except
    ``app.yaml`` (rewritten to declare exactly one strategy instance, so
    assertions in this suite don't have to filter a second instrument's
    positions/orders out of every query) and ``brokers.yaml`` (rewritten with
    ``synchronous: true`` for the simulation broker -- determinism, per the
    module docstring; the real committed ``brokers.yaml`` itself is exercised
    unmodified by ``test_dependency_container.py``, not re-verified here).
    """
    real_root = Path("configs")
    root = tmp_path / "configs"
    root.mkdir(exist_ok=True)  # idempotent: restart tests build a second
    # container against the same tmp_path to simulate a fresh process
    for name in ("accounts.yaml", "risk.yaml", "market_data.yaml"):
        shutil.copy(real_root / name, root / name)
    shutil.copytree(real_root / "strategies", root / "strategies", dirs_exist_ok=True)

    report_line = (
        f'report_output_dir: "{report_output_dir}"\n' if report_output_dir is not None else ""
    )
    (root / "app.yaml").write_text(
        f"""
environment: "development"
log_level: "INFO"
{report_line}instances:
  - strategy_id: strategy_1
    instrument: {instrument}
    account: {account}
""",
        encoding="utf-8",
    )

    (root / "brokers.yaml").write_text(
        """
active_broker: SIMULATION
rate_limits:
  ORDER_MUTATION: {max_calls: 100, per_seconds: 1.0}
  ORDER_READ: {max_calls: 100, per_seconds: 1.0}
  PORTFOLIO_READ: {max_calls: 100, per_seconds: 1.0}
  MARKET_DATA: {max_calls: 100, per_seconds: 1.0}
  INSTRUMENT_LOOKUP: {max_calls: 100, per_seconds: 1.0}
  GENERAL: {max_calls: 100, per_seconds: 1.0}
simulation:
  synchronous: true
  initial_cash: "10000000"
  connection_failure_probability: 0.0
  rejection_probability: 0.0
  partial_fill_probability: 0.0
  ack_latency_seconds: 0.0
  fill_latency_seconds: 0.0
kite:
  api_key_env_var: "KITE_API_KEY"
  api_secret_env_var: "KITE_API_SECRET"
  access_token_env_var: "KITE_ACCESS_TOKEN"
  read_retry_attempts: 3
  read_retry_delay_seconds: 0.0
  quote_batch_size: 200
""",
        encoding="utf-8",
    )
    return root


def build_nifty_option_chain(*, num_strikes_each_side: int = 5) -> InstrumentCatalog:
    return InstrumentCatalog.build_option_chain(
        underlying=NIFTY, exchange=Exchange.NFO, expiry=NIFTY_EXPIRY,
        atm_strike=NIFTY_ATM_STRIKE, strike_interval=NIFTY_STRIKE_INTERVAL,
        num_strikes_each_side=num_strikes_each_side, lot_size=NIFTY_LOT_SIZE,
    )


def atm_legs(catalog: InstrumentCatalog, *, instrument: str = NIFTY) -> tuple[InstrumentIdentifier, InstrumentIdentifier]:
    """The exact CE/PE ``InstrumentIdentifier``s Strategy1's own
    ``StrikeSelector`` will resolve for the fixed NIFTY scenario above --
    fetched from the same catalog, not hand-reconstructed, so this can never
    drift out of sync with ``InstrumentCatalog.build_option_chain``'s own
    symbol-naming convention."""
    exchange = Exchange.NFO if instrument == NIFTY else Exchange.BFO
    expiry = _EXPIRY[instrument]
    strike = NIFTY_ATM_STRIKE if instrument == NIFTY else SENSEX_ATM_STRIKE
    call = catalog.find_option(
        underlying=instrument, expiry=expiry, strike=strike, option_type=OptionType.CE, exchange=exchange
    )
    put = catalog.find_option(
        underlying=instrument, expiry=expiry, strike=strike, option_type=OptionType.PE, exchange=exchange
    )
    return (
        InstrumentIdentifier(exchange=call.exchange, tradingsymbol=call.tradingsymbol),
        InstrumentIdentifier(exchange=put.exchange, tradingsymbol=put.tradingsymbol),
    )


def build_container(
    tmp_path: Path,
    *,
    clock: MutableClock,
    db_path: Path,
    instrument_catalog: InstrumentCatalog | None = None,
    price_source: StaticPriceSource | None = None,
    instrument: str = NIFTY,
    report_output_dir: str | None = None,
    **overrides,
) -> DependencyContainer:
    """Build a fully-wired ``DependencyContainer`` against a file-backed
    SQLite DB (so calling this twice with the same ``db_path`` simulates a
    process restart against persistent state -- see
    ``test_crash_and_restart_recovery.py``), a real ``SimulationBroker``
    seeded with ``instrument_catalog``/``price_source``, and fakes only for
    the four seams with no concrete implementation anywhere in this codebase
    yet.

    ``build_engine`` is monkeypatched only for the duration of this call (via
    a context manager, not a fixture-scoped patch) because it is otherwise
    hardcoded to Postgres-only ``connect_args`` that SQLite's driver rejects
    -- exactly the same, already-documented reason
    ``test_dependency_container.py``'s ``patched_engine`` fixture exists.
    """
    root = make_config_root(tmp_path, instrument=instrument, report_output_dir=report_output_dir)

    def _fake_build_engine(settings):  # noqa: ANN001, ARG001
        # WAL mode + a busy timeout: this codebase's real, already-approved
        # design legitimately opens several short-lived sessions in sequence
        # within one logical operation (e.g. a risk check's own session, then
        # exit_logic's own session for the actual writes) -- entirely safe
        # against Postgres's MVCC in production (separate connections,
        # row-level locking), but SQLite's *default* rollback-journal mode
        # takes an exclusive lock for any writer and blocks against any other
        # connection still holding so much as an open read transaction. WAL
        # mode allows one writer alongside concurrent readers, which is what
        # actually matches Postgres's behavior closely enough for this
        # multi-session pattern to work reliably in a SQLite-backed test.
        #
        # (A single shared connection via StaticPool was tried first and
        # rejected: it introduces a *worse* problem than the one it solves --
        # two temporally-overlapping sessions sharing one physical connection
        # can corrupt each other's transaction state, since a single SQLite
        # connection only supports one active transaction at a time. WAL
        # mode's multi-connection concurrency is the correct fix.)
        engine = create_engine(f"sqlite:///{db_path}", connect_args={"timeout": 30})

        @event.listens_for(engine, "connect")
        def _set_wal_mode(dbapi_connection, connection_record):  # noqa: ANN001, ARG001
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.close()

        Base.metadata.create_all(engine)
        return engine

    kwargs: dict[str, object] = {
        "config_root": root,
        "instrument_service": FakeInstrumentService(),
        "expiry_service": FakeExpiryService(),
        "spot_price_provider": FakeSpotPriceProvider(),
        "tick_stream": FakeTickStream(),
        "simulation_instrument_catalog": instrument_catalog or InstrumentCatalog(),
        "simulation_price_source": price_source or StaticPriceSource({}),
        "time_provider": clock,
        # A very long poll interval: most tests in this suite drive triggers
        # manually (runner.dispatch_time_trigger()) against the same shared,
        # mutable clock the real scheduler background thread also reads --
        # without this, that thread can independently see the same trigger
        # become due and dispatch it concurrently with the test's own manual
        # call, racing on the same database rows.
        "scheduler_config": SchedulerConfig(poll_interval_seconds=3600),
        # Same reasoning for the monitoring heartbeat: tests that drive exits
        # by manually firing the cutoff trigger must not have the background
        # monitoring thread also evaluate (and possibly fire) the exit
        # concurrently. Tests that specifically exercise the heartbeat override
        # this with a short interval.
        "monitoring_scheduler_config": MonitoringSchedulerConfig(interval_seconds=3600),
    }
    kwargs.update(overrides)

    with patch.object(dependency_container_module, "build_engine", _fake_build_engine):
        return DependencyContainer(**kwargs)
