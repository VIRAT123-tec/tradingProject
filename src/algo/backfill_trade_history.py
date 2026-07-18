"""Operator CLI to backfill the ``trade_history`` analytics table:
``python -m algo.backfill_trade_history``.

Two jobs, both idempotent and safe to run any time:

* **Seed history for trades that predate this feature** -- every CLOSED position
  ever recorded gets its permanent ``trade_history`` row.
* **Repair any missed live write** -- because the live recorder is best-effort
  (a failure there never blocks a trade, by design), this command is the
  guaranteed way to reconcile ``trade_history`` back to the execution tables,
  which remain the single source of truth.

It walks every CLOSED position, skips those already recorded (the UNIQUE
``position_id`` makes this exact), and records the rest through the same
``TradeHistoryRecorder`` the live path uses -- so a backfilled row is identical
to one written at close time. ``mode`` (PAPER/LIVE) is inferred per trade from
its broker order ids. One bad row is logged and skipped, never aborting the run.

Read-only against the execution tables; only ever inserts into ``trade_history``.
Exit code 0 on success, 1 on error.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import TYPE_CHECKING

from algo.database.repositories.position_repository import PositionRepository
from algo.database.repositories.trade_history_repository import TradeHistoryRepository
from algo.services.trade_history_recorder import TradeHistoryRecorder

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session, sessionmaker

_logger = logging.getLogger("algo.backfill_trade_history")


def _run(session_factory: sessionmaker[Session], args: argparse.Namespace, *, out=sys.stdout) -> int:
    """Backfill missing history rows. Returns a process exit code."""
    with session_factory() as session:
        closed_ids = PositionRepository(session).list_closed_ids()
        already = TradeHistoryRepository(session).recorded_position_ids(closed_ids)
    missing = [pid for pid in closed_ids if pid not in already]

    print(
        f"CLOSED positions: {len(closed_ids)} | already recorded: {len(already)} | "
        f"to backfill: {len(missing)}",
        file=out,
    )
    if args.dry_run:
        print("dry-run: no rows written.", file=out)
        return 0

    # mode=None -> the recorder infers PAPER/LIVE per trade from its order ids.
    recorder = TradeHistoryRecorder(session_factory=session_factory, mode=None)
    written = 0
    for pid in missing:
        before = _exists(session_factory, pid)
        recorder.record_closed_position(pid)  # self-contained, exception-safe
        if not before and _exists(session_factory, pid):
            written += 1
    print(f"backfill complete: {written} history row(s) written.", file=out)
    return 0


def _exists(session_factory: sessionmaker[Session], position_id: int) -> bool:
    with session_factory() as session:
        return TradeHistoryRepository(session).get_by_position_id(position_id) is not None


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="algo.backfill_trade_history",
        description="Backfill the trade_history analytics table from CLOSED positions.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report how many rows would be backfilled without writing anything.",
    )
    return parser


def _build_session_factory() -> sessionmaker[Session]:
    from algo.database.database import build_engine, load_database_settings
    from algo.database.session import build_session_factory

    return build_session_factory(build_engine(load_database_settings()))


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    args = _build_parser().parse_args(argv)
    try:
        return _run(_build_session_factory(), args)
    except Exception as exc:  # noqa: BLE001 -- clean non-zero exit
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
