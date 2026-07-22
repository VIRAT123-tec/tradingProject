"""TradeHistoryRecorder: writes one permanent trade_history row after a
position closes.

It is a read-and-record side channel, exactly parallel to the Excel exporter
and equally isolated from trading: it reads the (already committed) CLOSED
position and its trades/orders, maps them -- plus a few derived analytics
fields -- onto a ``TradeHistory`` row, and inserts it idempotently. It never
raises into the caller; a failure here leaves the trade fully intact in the
execution tables (the source of truth) and can be rebuilt later by the backfill
command. See ``database/models/trade_history.py``.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING

from algo.common.enums import OptionType, OrderPurpose, OrderStatus, PositionState
from algo.common.utilities import pnl_per_share
from algo.database.models.trade_history import TradeHistory
from algo.database.repositories.account_repository import AccountRepository
from algo.database.repositories.order_repository import OrderRepository
from algo.database.repositories.position_repository import PositionRepository
from algo.database.repositories.strategy_instance_repository import StrategyInstanceRepository
from algo.database.repositories.trade_history_repository import TradeHistoryRepository
from algo.database.session import unit_of_work

if TYPE_CHECKING:
    from sqlalchemy.orm import Session, sessionmaker

    from algo.database.models.position import Position
    from algo.database.models.trade import Trade


class TradeHistoryRecorder:
    """Records completed trades into ``trade_history``. Structurally satisfies
    ``strategy_context.TradeHistoryRecorder``."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        mode: str | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._mode = mode  # "PAPER" / "LIVE" (the active broker at run time)
        self._logger = logger if logger is not None else logging.getLogger("algo.trade_history")

    def record_closed_position(self, position_id: int) -> None:
        """Record the history row for a just-closed position. Exception-safe by
        contract: any failure is logged and swallowed so trade-history recording
        can never disturb the exit that produced the data (the caller in
        ``exit_logic`` also guards it -- defence in depth)."""
        try:
            with unit_of_work(self._session_factory) as session:
                created = self._record(session, position_id)
            if created:
                self._logger.info("trade_history recorded for position %d", position_id)
            else:
                self._logger.debug("trade_history already present for position %d; skipped", position_id)
        except Exception:  # noqa: BLE001 -- history recording must never break trading
            self._logger.error(
                "failed to record trade_history for position %d; the trade is safe "
                "in the execution tables and can be backfilled",
                position_id,
                exc_info=True,
            )

    def _record(self, session: Session, position_id: int) -> bool:
        """Build and idempotently insert the history row. Returns True if a new
        row was created, False if one already existed. Raises only on a genuine,
        unexpected persistence error (caught by ``record_closed_position``)."""
        history_repo = TradeHistoryRepository(session)
        if history_repo.get_by_position_id(position_id) is not None:
            return False  # already recorded -- idempotent no-op

        position_repo = PositionRepository(session)
        position = position_repo.get_by_id(position_id)
        if position is None or position.state is not PositionState.CLOSED:
            self._logger.warning(
                "trade_history: position %d missing or not CLOSED (state=%s); skipping",
                position_id, None if position is None else position.state.value,
            )
            return False

        row = self._build_row(session, position)
        _, created = history_repo.add_if_absent(row)
        return created

    def _build_row(self, session: Session, position: Position) -> TradeHistory:
        instance = StrategyInstanceRepository(session).get_by_id(position.strategy_instance_id)
        account = (
            AccountRepository(session).get_by_id(instance.account_id)
            if instance is not None else None
        )
        trades = PositionRepository(session).list_trades_for_position(position.id)
        by_type = {t.option_type: t for t in trades}
        call = by_type.get(OptionType.CE)
        put = by_type.get(OptionType.PE)

        broker_order_ids = self._broker_order_ids(session, trades)
        # Live recording passes the active broker's mode explicitly. The
        # backfill constructs the recorder with mode=None, so infer it per trade
        # from the order ids (SimulationBroker mints 'SIM...' ids).
        mode = self._mode or _infer_mode(broker_order_ids)

        entry_time = position.entry_completed_at
        exit_time = position.exit_completed_at
        holding_seconds = (
            int((exit_time - entry_time).total_seconds())
            if entry_time is not None and exit_time is not None else None
        )
        holding_minutes = (
            (Decimal(holding_seconds) / Decimal("60")).quantize(Decimal("0.01"))
            if holding_seconds is not None else None
        )

        return TradeHistory(
            position_id=position.id,
            strategy_instance_id=position.strategy_instance_id,
            account_id=instance.account_id if instance is not None else None,
            trade_date=position.trade_date,
            strategy_id=instance.strategy_id if instance is not None else "unknown",
            instrument=instance.instrument if instance is not None else "unknown",
            account_name=account.display_name if account is not None else None,
            mode=mode,
            exchange=instance.exchange if instance is not None else None,
            strike=position.strike,
            expiry_date=position.expiry_date,
            call_symbol=call.trading_symbol if call is not None else None,
            put_symbol=put.trading_symbol if put is not None else None,
            entry_time=entry_time,
            exit_time=exit_time,
            entry_signal_time=position.entry_signal_time,
            exit_signal_time=position.exit_signal_time,
            holding_seconds=holding_seconds,
            holding_minutes=holding_minutes,
            lots=position.lots,
            lot_size=position.lot_size,
            quantity=position.quantity,
            combined_entry_premium=position.combined_entry_premium,
            combined_exit_premium=position.combined_exit_premium,
            target_premium=position.target_premium,
            stoploss_premium=position.stoploss_premium,
            target_pct=position.target_pct,
            sl_pct=position.sl_pct,
            call_entry_price=call.entry_price if call is not None else None,
            call_exit_price=call.exit_price if call is not None else None,
            put_entry_price=put.entry_price if put is not None else None,
            put_exit_price=put.exit_price if put is not None else None,
            entry_spot_ltp=position.entry_spot_ltp,
            exit_spot_ltp=None,  # not persisted by execution yet (later phase)
            exit_reason=position.exit_reason,
            realized_pnl=position.realized_pnl,
            pnl_per_share=pnl_per_share(position.realized_pnl, position.lot_size),
            profit_percent=_profit_percent(position),
            max_profit_seen=None,  # not persisted by monitoring yet (later phase)
            max_loss_seen=None,
            day_of_week=position.trade_date.strftime("%A"),
            month=position.trade_date.month,
            broker_order_ids=broker_order_ids,
        )

    @staticmethod
    def _broker_order_ids(session: Session, trades: list[Trade]) -> dict | None:
        """Map completed entry/exit broker order ids per leg:
        {'entry': {'CE': id, 'PE': id}, 'exit': {'CE': id, 'PE': id}}."""
        order_repo = OrderRepository(session)
        result: dict[str, dict[str, str]] = {}
        for trade in trades:
            leg = trade.option_type.value  # 'CE' / 'PE'
            for order in order_repo.list_for_trade(trade.id):
                if order.status is not OrderStatus.COMPLETE or not order.broker_order_id:
                    continue
                bucket = "entry" if order.purpose is OrderPurpose.ENTRY else "exit"
                result.setdefault(bucket, {})[leg] = order.broker_order_id
        return result or None


def _infer_mode(broker_order_ids: dict | None) -> str | None:
    """Infer PAPER vs LIVE from the recorded broker order ids: the simulation
    broker mints 'SIM...' ids, a real broker does not. Returns None if there are
    no ids to judge from."""
    if not broker_order_ids:
        return None
    ids = [oid for leg in broker_order_ids.values() for oid in leg.values()]
    if not ids:
        return None
    return "PAPER" if any(str(oid).startswith("SIM") for oid in ids) else "LIVE"


def _profit_percent(position: Position) -> Decimal | None:
    """realized_pnl as a percent of premium collected. None if the inputs
    needed to compute it are absent or zero."""
    pnl = position.realized_pnl
    entry = position.combined_entry_premium
    qty = position.quantity
    if pnl is None or entry is None or qty is None:
        return None
    collected = entry * qty
    if collected == 0:
        return None
    return (pnl / collected * Decimal("100")).quantize(Decimal("0.0001"))
