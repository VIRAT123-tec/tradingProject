"""Per-leg trade report exporter: writes one ``.xlsx`` file per trading day,
one row per option leg, from the trades already persisted by the strategy.

This is a pure *read-and-render* side channel. It owns no trading decision,
places no order, and writes nothing back to the trading tables -- it only reads
CLOSED positions and their legs and renders them to an Excel workbook. It is
invoked after a position reaches CLOSED (see ``exit_logic._finalize_closed``),
always behind an exception guard, so a reporting failure can never affect the
exit that produced the data.

Columns, in exact order:

    S.no | Date | Instrument | Entry Time | Entry Price | Exit Time | Exit Price
    | Exit Reason | P&L (₹) | Total Profit (₹) | Total Loss (₹) | Net P&L (₹)

* **Date** -- the position's ``trade_date`` as ``DD-MM-YYYY``.
* **Instrument** -- rebuilt (never read from the stored broker symbol, whose
  format is broker-specific) as
  ``<Exchange>:<Underlying><YY><MM><DD><Strike><OptionType>`` from the leg's
  exchange, the instance's underlying, the position's *expiry* date, the leg's
  strike, and the leg's option type -- e.g. ``NFO:NIFTY26071424200CE``.
* **Entry Time / Exit Time** -- the leg's own fill timestamps, converted from
  the stored UTC to IST and rendered ``HH:MM:SS``.
* **Entry Price** -- the leg's average executed SELL price (a short straddle
  sells to open); **Exit Price** -- the average executed BUY price (buys to
  close). Both are the ``Trade.entry_price``/``exit_price`` the strategy copied
  from the winning fill; nothing is recomputed here. Rendered to two decimals.

The last five columns are **trade-level** (per completed position), reusing
values the strategy already produced -- nothing is recomputed:

* **Exit Reason** -- the position's ``exit_reason`` (TARGET / STOPLOSS / TIMEOUT
  / KILL_SWITCH / MANUAL / ...).
* **P&L (₹)** -- the position's own ``realized_pnl`` (this completed trade only).
* **Total Profit (₹)** -- running cumulative sum of only the *profitable* trades
  (losses never decrease it).
* **Total Loss (₹)** -- running cumulative *absolute* loss (positive, for
  readability).
* **Net P&L (₹)** -- running cumulative net = Total Profit - Total Loss.

Because the report is per-leg, both leg rows of a trade carry the same
trade-level values (exactly as Exit Reason is shared) and the running totals
advance once per trade. The totals **continue across days**: they are seeded
from the sum of every earlier CLOSED position's ``realized_pnl`` (the same
database this exporter already reads), so a per-day rebuild never restarts them
and the file-per-day design stays intact.

One file per trading day: every call rebuilds that day's whole workbook from
the database, so it is idempotent (re-exporting yields the same file) and a
day with several closed positions -- across every instrument -- accumulates
into the single ``trades_<DD-MM-YYYY>.xlsx`` with ``S.no`` running 1..N.
Concurrent closes (Nifty and Sensex can finalize on separate threads) are
serialised by an instance lock so the two never write the file at once.
"""

from __future__ import annotations

import logging
import threading
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

import pytz
from openpyxl import Workbook
from sqlalchemy import select

from algo.common.enums import OptionType, PositionState
from algo.database.models.position import Position
from algo.database.models.strategy_instance import StrategyInstance
from algo.database.models.trade import Trade

if TYPE_CHECKING:
    from sqlalchemy.orm import Session, sessionmaker

_IST = pytz.timezone("Asia/Kolkata")

_COLUMNS = (
    "S.no", "Date", "Instrument", "Entry Time", "Entry Price", "Exit Time", "Exit Price",
    "Exit Reason", "P&L (₹)", "Total Profit (₹)", "Total Loss (₹)", "Net P&L (₹)",
)
# 1-indexed columns rendered as money (two decimals): Entry Price, Exit Price,
# P&L, Total Profit, Total Loss, Net P&L.
_MONEY_COLUMNS = (5, 7, 9, 10, 11, 12)

# CE before PE, matching the report's leg ordering example.
_LEG_ORDER = {OptionType.CE: 0, OptionType.PE: 1}


def format_instrument(
    *, exchange: str, underlying: str, expiry: date, strike: Decimal, option_type: OptionType
) -> str:
    """Build the exact required symbol
    ``<Exchange>:<Underlying><YY><MM><DD><Strike><OptionType>`` -- e.g.
    ``NFO:NIFTY26071424200CE``. The strike is rendered as a plain integer
    (Nifty/Sensex strikes are always whole), the expiry as ``YYMMDD``."""
    return f"{exchange}:{underlying}{expiry:%y%m%d}{int(strike)}{option_type.value}"


def _format_time_ist(dt: datetime) -> str:
    """Render a stored fill timestamp as IST ``HH:MM:SS``. Stored times are
    timezone-aware UTC; a naive value (should not occur) is treated as UTC so
    this never raises."""
    aware = dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    return aware.astimezone(_IST).strftime("%H:%M:%S")


class _LegRow:
    """One rendered row -- plain values extracted while the session is open, so
    nothing lazy-loads after it closes. The last five fields are trade-level
    (shared by both legs of a position): the cumulative totals are the running
    state *after* this trade."""

    __slots__ = (
        "instrument", "trade_date", "entry_time", "entry_price", "exit_time", "exit_price",
        "exit_reason", "pnl", "total_profit", "total_loss", "net_pnl",
    )

    def __init__(
        self,
        *,
        instrument: str,
        trade_date: date,
        entry_time: datetime,
        entry_price: Decimal,
        exit_time: datetime,
        exit_price: Decimal,
        exit_reason: str,
        pnl: Decimal,
        total_profit: Decimal,
        total_loss: Decimal,
        net_pnl: Decimal,
    ) -> None:
        self.instrument = instrument
        self.trade_date = trade_date
        self.entry_time = entry_time
        self.entry_price = entry_price
        self.exit_time = exit_time
        self.exit_price = exit_price
        self.exit_reason = exit_reason
        self.pnl = pnl
        self.total_profit = total_profit
        self.total_loss = total_loss
        self.net_pnl = net_pnl


class TradeReportExporter:
    """Renders completed trades to a daily Excel workbook. Structurally
    satisfies ``strategy_context.TradeExporter``."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        output_dir: Path,
        logger: logging.Logger | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._output_dir = Path(output_dir)
        self._logger = logger if logger is not None else logging.getLogger("algo.reporting")
        # Serialises concurrent day-file rebuilds (e.g. Nifty and Sensex
        # finalising their exits on separate threads at the cutoff).
        self._write_lock = threading.Lock()

    def export_closed_position(self, position_id: int) -> None:
        """Entry point the trading path calls after a position reaches CLOSED.

        Exception-safe by contract: it resolves the position's trading day and
        rebuilds that day's report, but any failure is logged and swallowed so
        reporting can never disturb the exit that triggered it (the caller in
        ``exit_logic`` also guards it -- this is defence in depth)."""
        try:
            trade_date = self._trade_date_for(position_id)
            if trade_date is None:
                self._logger.warning(
                    "trade report: position %d not found or has no trade_date; skipping export",
                    position_id,
                )
                return
            path = self.export_day(trade_date)
            self._logger.info(
                "trade report written for %s: %s", trade_date.isoformat(), path
            )
        except Exception:  # noqa: BLE001 -- reporting must never break trading
            self._logger.error(
                "trade report export failed for position %d", position_id, exc_info=True
            )

    def export_day(self, trade_date: date) -> Path:
        """Rebuild the whole workbook for one trading day from the database and
        write ``<output_dir>/trades_<DD-MM-YYYY>.xlsx``. Returns the path.

        Serialised by ``self._write_lock`` so two concurrent closes on the same
        day cannot render/write the file simultaneously."""
        with self._write_lock:
            rows = self._collect_rows(trade_date)
            return self._write_workbook(trade_date, rows)

    # -- Internal ------------------------------------------------------

    def _trade_date_for(self, position_id: int) -> date | None:
        with self._session_factory() as session:
            position = session.get(Position, position_id)
            return position.trade_date if position is not None else None

    def _collect_rows(self, trade_date: date) -> list[_LegRow]:
        """Read every CLOSED position for the day (across all instruments) and
        flatten it to one ``_LegRow`` per completed leg, ordered by position
        then CE-before-PE. All values are extracted here, inside the session."""
        rows: list[_LegRow] = []
        with self._session_factory() as session:
            # Seed the running totals from every earlier day's closed trades so
            # the cumulative columns continue across days (never restart).
            total_profit, total_loss = self._opening_cumulative(session, trade_date)

            positions = (
                session.execute(
                    select(Position)
                    .where(
                        Position.trade_date == trade_date,
                        Position.state == PositionState.CLOSED,
                    )
                    .order_by(Position.exit_completed_at, Position.id)
                )
                .scalars()
                .all()
            )
            for position in positions:
                instance = session.get(StrategyInstance, position.strategy_instance_id)
                if instance is None or position.expiry_date is None:
                    self._logger.warning(
                        "trade report: position %d missing instance/expiry; skipping",
                        position.id,
                    )
                    continue
                # Advance the running totals once per trade (per position),
                # reusing the position's already-computed realized P&L.
                pnl = position.realized_pnl if position.realized_pnl is not None else Decimal("0")
                if pnl > 0:
                    total_profit += pnl
                elif pnl < 0:
                    total_loss += -pnl
                net_pnl = total_profit - total_loss
                exit_reason = position.exit_reason.value if position.exit_reason is not None else ""

                legs = sorted(
                    session.execute(
                        select(Trade).where(Trade.position_id == position.id)
                    ).scalars(),
                    key=lambda t: _LEG_ORDER.get(t.option_type, 99),
                )
                for leg in legs:
                    if (
                        leg.entry_price is None
                        or leg.exit_price is None
                        or leg.entry_time is None
                        or leg.exit_time is None
                    ):
                        # A CLOSED position should have both legs fully priced;
                        # skip (rather than emit blanks) if one somehow is not.
                        self._logger.warning(
                            "trade report: leg %d of position %d is not fully priced; skipping",
                            leg.id,
                            position.id,
                        )
                        continue
                    rows.append(
                        _LegRow(
                            instrument=format_instrument(
                                exchange=leg.exchange.value,
                                underlying=instance.instrument,
                                expiry=position.expiry_date,
                                strike=leg.strike,
                                option_type=leg.option_type,
                            ),
                            trade_date=position.trade_date,
                            entry_time=leg.entry_time,
                            entry_price=leg.entry_price,
                            exit_time=leg.exit_time,
                            exit_price=leg.exit_price,
                            exit_reason=exit_reason,
                            pnl=pnl,
                            total_profit=total_profit,
                            total_loss=total_loss,
                            net_pnl=net_pnl,
                        )
                    )
        return rows

    def _opening_cumulative(self, session: Session, trade_date: date) -> tuple[Decimal, Decimal]:
        """Cumulative (profit, absolute-loss) of every CLOSED trade *before* this
        day, so the day's running totals continue across days rather than
        restarting. Reuses each position's already-computed ``realized_pnl`` --
        nothing is recomputed here."""
        pnls = session.execute(
            select(Position.realized_pnl).where(
                Position.state == PositionState.CLOSED,
                Position.trade_date < trade_date,
            )
        ).scalars()
        profit = Decimal("0")
        loss = Decimal("0")
        for pnl in pnls:
            if pnl is None:
                continue
            if pnl > 0:
                profit += pnl
            elif pnl < 0:
                loss += -pnl
        return profit, loss

    def _write_workbook(self, trade_date: date, rows: list[_LegRow]) -> Path:
        self._output_dir.mkdir(parents=True, exist_ok=True)
        path = self._output_dir / f"trades_{trade_date:%d-%m-%Y}.xlsx"

        workbook = Workbook()
        sheet = workbook.active
        sheet.title = "Trades"
        sheet.append(list(_COLUMNS))

        for serial, row in enumerate(rows, start=1):
            sheet.append(
                [
                    serial,
                    f"{row.trade_date:%d-%m-%Y}",
                    row.instrument,
                    _format_time_ist(row.entry_time),
                    float(row.entry_price.quantize(Decimal("0.01"))),
                    _format_time_ist(row.exit_time),
                    float(row.exit_price.quantize(Decimal("0.01"))),
                    row.exit_reason,
                    float(row.pnl.quantize(Decimal("0.01"))),
                    float(row.total_profit.quantize(Decimal("0.01"))),
                    float(row.total_loss.quantize(Decimal("0.01"))),
                    float(row.net_pnl.quantize(Decimal("0.01"))),
                ]
            )
            # Always show two decimals on every money column.
            for col in _MONEY_COLUMNS:
                sheet.cell(row=serial + 1, column=col).number_format = "0.00"

        workbook.save(path)
        return path
