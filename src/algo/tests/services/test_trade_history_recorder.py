"""Tests for the trade-history recorder and its backfill command.

Builds real CLOSED positions (with trades + orders) in in-memory SQLite and
records them, asserting the mapped row, the derived analytics fields, strict
idempotency, exception-isolation, and that only CLOSED positions are recorded.
"""

from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
from decimal import Decimal

from sqlalchemy import BigInteger, create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool


@compiles(BigInteger, "sqlite")
def _bigint_sqlite(type_, compiler, **kw):  # noqa: ANN001, ANN202
    return "INTEGER"


@compiles(JSONB, "sqlite")
def _jsonb_sqlite(type_, compiler, **kw):  # noqa: ANN001, ANN202
    return "JSON"


from algo.backfill_trade_history import _run as backfill_run
from algo.common.enums import (
    Exchange,
    ExitReason,
    OptionType,
    OrderPurpose,
    OrderStatus,
    OrderType,
    PositionState,
    TradeLegStatus,
    TransactionType,
)
from algo.database.models import (
    Account,
    Base,
    Order,
    Position,
    StrategyInstance,
    Trade,
)
from algo.database.repositories.trade_history_repository import TradeHistoryRepository
from algo.services.trade_history_recorder import TradeHistoryRecorder

TRADE_DATE = date(2026, 7, 13)  # a Monday
EXPIRY = date(2026, 7, 14)
ENTRY_UTC = datetime(2026, 7, 13, 3, 50, 0, tzinfo=timezone.utc)      # 09:20 IST
EXIT_UTC = datetime(2026, 7, 13, 4, 50, 0, tzinfo=timezone.utc)       # 10:20 IST (60 min later)


def _factory():
    engine = create_engine(
        "sqlite://", poolclass=StaticPool, connect_args={"check_same_thread": False}
    )
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


def _make_closed_position(
    sf,
    *,
    instrument="NIFTY",
    exchange=Exchange.NFO,
    state=PositionState.CLOSED,
    sim_orders=True,
    entry_prem=Decimal("200"),
    exit_prem=Decimal("160"),
    realized=Decimal("2600"),
):
    with sf() as s:
        acct = Account(broker="SIMULATION", display_name="Test Acct")
        s.add(acct)
        s.flush()
        inst = StrategyInstance(
            strategy_id="strategy_1", instrument=instrument, account_id=acct.id, exchange=exchange
        )
        s.add(inst)
        s.flush()
        pos = Position(
            strategy_instance_id=inst.id, trade_date=TRADE_DATE, state=state,
            expiry_date=EXPIRY, strike=Decimal("24150"), strike_interval=Decimal("50"),
            lots=1, lot_size=65, quantity=65, entry_spot_ltp=Decimal("24137.55"),
            target_pct=Decimal("0.10"), sl_pct=Decimal("0.10"),
            combined_entry_premium=entry_prem, target_premium=Decimal("180"),
            stoploss_premium=Decimal("220"), combined_exit_premium=exit_prem,
            exit_reason=ExitReason.TIMEOUT, realized_pnl=realized,
            entry_completed_at=ENTRY_UTC, exit_completed_at=EXIT_UTC,
        )
        s.add(pos)
        s.flush()
        prefix = "SIM" if sim_orders else "251216"
        for opt, ce_entry, ce_exit in (
            (OptionType.CE, Decimal("110"), Decimal("90")),
            (OptionType.PE, Decimal("90"), Decimal("70")),
        ):
            trade = Trade(
                position_id=pos.id, option_type=opt, trading_symbol=f"{instrument}{opt.value}",
                exchange=exchange, strike=Decimal("24150"), quantity=65,
                entry_price=ce_entry, exit_price=ce_exit, status=TradeLegStatus.CLOSED,
            )
            s.add(trade)
            s.flush()
            for purpose, ttype, oid in (
                # Globally unique per position (orders.broker_order_id is UNIQUE).
                (OrderPurpose.ENTRY, TransactionType.SELL, f"{prefix}-{pos.id}-E-{opt.value}"),
                (OrderPurpose.EXIT, TransactionType.BUY, f"{prefix}-{pos.id}-X-{opt.value}"),
            ):
                s.add(Order(
                    trade_id=trade.id, purpose=purpose, transaction_type=ttype,
                    order_type=OrderType.MARKET, quantity=65, status=OrderStatus.COMPLETE,
                    broker_order_id=oid,
                ))
        s.commit()
        return pos.id


def _row(sf, position_id):
    with sf() as s:
        return TradeHistoryRepository(s).get_by_position_id(position_id)


class TestRecordsClosedTrade:
    def test_maps_all_core_fields(self):
        sf = _factory()
        pid = _make_closed_position(sf)
        TradeHistoryRecorder(session_factory=sf, mode="PAPER").record_closed_position(pid)

        r = _row(sf, pid)
        assert r is not None
        assert r.position_id == pid
        assert r.trade_date == TRADE_DATE
        assert r.strategy_id == "strategy_1"
        assert r.instrument == "NIFTY"
        assert r.account_name == "Test Acct"
        assert r.mode == "PAPER"
        assert r.exchange is Exchange.NFO
        assert r.strike == Decimal("24150.00")
        assert r.expiry_date == EXPIRY
        assert r.call_symbol == "NIFTYCE" and r.put_symbol == "NIFTYPE"
        assert r.quantity == 65 and r.lot_size == 65 and r.lots == 1
        assert r.combined_entry_premium == Decimal("200.0000")
        assert r.combined_exit_premium == Decimal("160.0000")
        assert r.exit_reason is ExitReason.TIMEOUT
        assert r.realized_pnl == Decimal("2600.0000")
        assert r.call_entry_price == Decimal("110.0000")
        assert r.put_exit_price == Decimal("70.0000")
        assert r.entry_spot_ltp == Decimal("24137.5500")

    def test_derived_fields(self):
        sf = _factory()
        pid = _make_closed_position(sf)
        TradeHistoryRecorder(session_factory=sf, mode="PAPER").record_closed_position(pid)
        r = _row(sf, pid)
        assert r.holding_seconds == 3600
        assert r.holding_minutes == Decimal("60.00")
        # profit_percent = 2600 / (200 * 65) * 100 = 20.0000
        assert r.profit_percent == Decimal("20.0000")
        assert r.day_of_week == "Monday"
        assert r.month == 7

    def test_nullable_future_fields_are_null(self):
        sf = _factory()
        pid = _make_closed_position(sf)
        TradeHistoryRecorder(session_factory=sf, mode="PAPER").record_closed_position(pid)
        r = _row(sf, pid)
        assert r.exit_spot_ltp is None
        assert r.max_profit_seen is None and r.max_loss_seen is None

    def test_broker_order_ids_grouped_by_purpose_and_leg(self):
        sf = _factory()
        pid = _make_closed_position(sf)
        TradeHistoryRecorder(session_factory=sf, mode="PAPER").record_closed_position(pid)
        r = _row(sf, pid)
        assert set(r.broker_order_ids) == {"entry", "exit"}
        assert set(r.broker_order_ids["entry"]) == {"CE", "PE"}
        assert set(r.broker_order_ids["exit"]) == {"CE", "PE"}
        assert r.broker_order_ids["entry"]["CE"] == f"SIM-{pid}-E-CE"
        assert r.broker_order_ids["exit"]["PE"] == f"SIM-{pid}-X-PE"

    def test_mode_inferred_from_order_ids_when_not_given(self):
        sf = _factory()
        pid_paper = _make_closed_position(sf, sim_orders=True)
        pid_live = _make_closed_position(sf, instrument="SENSEX", exchange=Exchange.BFO, sim_orders=False)
        rec = TradeHistoryRecorder(session_factory=sf, mode=None)  # infer per trade
        rec.record_closed_position(pid_paper)
        rec.record_closed_position(pid_live)
        assert _row(sf, pid_paper).mode == "PAPER"
        assert _row(sf, pid_live).mode == "LIVE"


class TestIdempotencyAndIsolation:
    def test_recording_twice_creates_one_row(self):
        sf = _factory()
        pid = _make_closed_position(sf)
        rec = TradeHistoryRecorder(session_factory=sf, mode="PAPER")
        rec.record_closed_position(pid)
        rec.record_closed_position(pid)  # again
        with sf() as s:
            from sqlalchemy import func, select
            from algo.database.models import TradeHistory
            count = s.execute(
                select(func.count()).select_from(TradeHistory).where(TradeHistory.position_id == pid)
            ).scalar()
        assert count == 1

    def test_non_closed_position_is_skipped(self):
        sf = _factory()
        pid = _make_closed_position(sf, state=PositionState.OPEN)
        TradeHistoryRecorder(session_factory=sf, mode="PAPER").record_closed_position(pid)
        assert _row(sf, pid) is None

    def test_missing_position_never_raises(self):
        sf = _factory()
        # Must not raise even for an unknown id -- recording is best-effort.
        TradeHistoryRecorder(session_factory=sf, mode="PAPER").record_closed_position(9999)
        assert _row(sf, 9999) is None

    def test_recording_failure_is_swallowed(self):
        sf = _factory()
        pid = _make_closed_position(sf)
        rec = TradeHistoryRecorder(session_factory=sf, mode="PAPER")
        # Break the build so the write raises internally; record_closed_position
        # must still not propagate.
        rec._build_row = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))  # noqa: SLF001
        rec.record_closed_position(pid)  # must not raise
        assert _row(sf, pid) is None


class TestBackfill:
    def test_backfill_seeds_missing_and_skips_existing(self):
        sf = _factory()
        p1 = _make_closed_position(sf)
        p2 = _make_closed_position(sf, instrument="BANKNIFTY")
        _make_closed_position(sf, instrument="FINNIFTY", state=PositionState.OPEN)  # not CLOSED

        # First pass writes both closed ones.
        backfill_run(sf, argparse.Namespace(dry_run=False))
        assert _row(sf, p1) is not None and _row(sf, p2) is not None

        # Second pass is a no-op (idempotent) -- still one row each.
        backfill_run(sf, argparse.Namespace(dry_run=False))
        with sf() as s:
            from sqlalchemy import func, select
            from algo.database.models import TradeHistory
            total = s.execute(select(func.count()).select_from(TradeHistory)).scalar()
        assert total == 2  # the OPEN position was never recorded

    def test_dry_run_writes_nothing(self):
        sf = _factory()
        p1 = _make_closed_position(sf)
        backfill_run(sf, argparse.Namespace(dry_run=True))
        assert _row(sf, p1) is None
