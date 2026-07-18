"""Operator CLI for StrategyInstance administration:
``python -m algo.instance_admin <command>``.

Today this covers exactly one operation: clearing a stale ``FROZEN`` status.

Why this exists: ``StrategyRunner`` freezes a strategy instance's persisted
``StrategyInstance.status`` (see ``strategy_runner.py``'s ``_freeze_instance``)
the moment any hook (most commonly the entry trigger) raises an unhandled
exception -- a deliberate fault-isolation safety net, not a bug. FROZEN is
semi-terminal by design (see ``state_machine.py``'s module docstring): no
*automated* logic ever clears it, on purpose, so a real, still-live problem
can never silently resume on the next restart. Only a human, who has actually
looked at *why* it froze, should clear it -- which is exactly what this CLI
requires you to do explicitly, never automatically.

This connects to the same database the platform uses and edits the row
directly; it does not need the platform process running (unlike
``killswitch.py``, which acts on an already-running process via a flag it
polls). A restart alone would never pick up a status change made any other
way, since nothing in the platform touches this column except the freeze
path itself.

Examples::

    python -m algo.instance_admin list
    python -m algo.instance_admin unfreeze --instance-id 1 --reason "confirmed stale: broker was never seeded with paper option data" --by operator

Exit code is 0 on success, 1 on error, so it composes in shell scripts. The
command logic lives in ``_run`` (which takes an injected session factory) so
it is unit-testable against a real (SQLite) database with no real Postgres.
"""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING

from algo.common.enums import InstanceStatus
from algo.database.repositories.strategy_instance_repository import (
    StrategyInstanceRepository,
)
from algo.database.session import unit_of_work

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session, sessionmaker


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="algo.instance_admin",
        description="Inspect and administer StrategyInstance rows (currently: clearing a stale FROZEN status).",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="List every strategy instance and its current status.")

    p = sub.add_parser(
        "unfreeze",
        help="Clear a FROZEN instance's status back to ACTIVE, after confirming the cause is stale/resolved.",
    )
    p.add_argument("--instance-id", type=int, required=True, help="StrategyInstance.id (see 'list').")
    p.add_argument(
        "--reason", required=True,
        help="Why this freeze is being cleared -- printed/logged for the operator's own audit trail "
        "(there is no dedicated database column for this; StrategyInstance has no notes field).",
    )
    p.add_argument("--by", dest="actor", default="operator", help="Who is performing this action.")
    return parser


def _run(session_factory: sessionmaker[Session], args: argparse.Namespace, *, out=sys.stdout) -> int:
    """Execute one parsed command. Returns a process exit code."""
    if args.command == "list":
        with session_factory() as session:
            instances = StrategyInstanceRepository(session).list_active() + StrategyInstanceRepository(
                session
            ).list_frozen()
            # list_active/list_frozen only cover ACTIVE/FROZEN; DISABLED
            # instances (if any) are intentionally not shown here -- this
            # command exists to spot stale FROZEN rows, not to be a general
            # table dump.
        if not instances:
            print("No ACTIVE or FROZEN strategy instances found.", file=out)
            return 0
        for inst in sorted(instances, key=lambda i: i.id):
            print(
                f"  id={inst.id} {inst.strategy_id}/{inst.instrument}@account={inst.account_id} "
                f"status={inst.status.value}",
                file=out,
            )
        return 0

    # unfreeze
    with unit_of_work(session_factory) as session:
        repo = StrategyInstanceRepository(session)
        instance = repo.get_by_id_for_update(args.instance_id)
        if instance is None:
            print(f"error: no StrategyInstance with id={args.instance_id}", file=out)
            return 1
        if instance.status is not InstanceStatus.FROZEN:
            print(
                f"instance {args.instance_id} ({instance.strategy_id}/{instance.instrument}) is "
                f"{instance.status.value}, not FROZEN -- no change.",
                file=out,
            )
            return 0
        repo.set_status(instance, InstanceStatus.ACTIVE)
        print(
            f"instance {args.instance_id} ({instance.strategy_id}/{instance.instrument}) reset "
            f"FROZEN -> ACTIVE by {args.actor}: {args.reason}",
            file=out,
        )
    return 0


def _build_session_factory() -> sessionmaker[Session]:
    """Build a session factory bound to the configured database (DATABASE_URL).
    Lazy imports keep the CLI's import cost (and its DB connection) out of the
    unit-test path, which drives ``_run`` against an injected factory instead."""
    from algo.database.database import build_engine, load_database_settings
    from algo.database.session import build_session_factory

    return build_session_factory(build_engine(load_database_settings()))


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        session_factory = _build_session_factory()
        return _run(session_factory, args)
    except Exception as exc:  # noqa: BLE001 -- turn any failure into a clean non-zero exit
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
