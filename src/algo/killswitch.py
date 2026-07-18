"""Operator CLI for the kill switch: ``python -m algo.killswitch <command>``.

This is the out-of-process "big red button". It connects to the same database
the running platform uses and engages/clears a control flag; the running
platform sees it on its next ``is_halted`` check (every monitoring heartbeat)
and stops taking new entries and flattens open positions via the normal exit
path. It works against an already-running process -- you do not restart the
platform to hit the kill switch.

Examples::

    python -m algo.killswitch status
    python -m algo.killswitch engage --reason "manual halt" --by ops
    python -m algo.killswitch disengage --by ops

Exit code is 0 on success, 1 on error, so it composes in shell scripts.
The command logic lives in ``_run`` (which takes an injected ``KillSwitch``)
so it is unit-testable without a real database.
"""

from __future__ import annotations

import argparse
import sys
from typing import TYPE_CHECKING

from algo.common.enums import RiskFlagScope, RiskFlagType

if TYPE_CHECKING:
    from collections.abc import Sequence

    from algo.risk.kill_switch import KillSwitch


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="algo.killswitch", description="Engage, clear, or inspect the platform kill switch."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    for name in ("engage", "disengage"):
        p = sub.add_parser(name, help=f"{name} a control flag")
        p.add_argument("--type", dest="flag_type", default="KILL_SWITCH",
                       choices=[t.value for t in RiskFlagType])
        p.add_argument("--scope", default="GLOBAL", choices=[s.value for s in RiskFlagScope])
        p.add_argument("--account-id", type=int, default=None)
        p.add_argument("--instance-id", type=int, default=None)
        p.add_argument("--by", dest="actor", default="operator",
                       help="Who is performing this action (recorded on the flag).")
        if name == "engage":
            p.add_argument("--reason", required=True, help="Why the kill switch is being engaged.")

    sub.add_parser("status", help="List active control flags")
    return parser


def _run(kill_switch: KillSwitch, args: argparse.Namespace, *, out=sys.stdout) -> int:
    """Execute one parsed command against an injected KillSwitch. Returns a
    process exit code."""
    if args.command == "status":
        flags = kill_switch.list_active()
        if not flags:
            print("No active control flags. Trading is NOT halted.", file=out)
            return 0
        print(f"{len(flags)} active control flag(s) -- trading IS halted:", file=out)
        for f in flags:
            target = (
                f"account={f.account_id}" if f.account_id is not None
                else f"instance={f.strategy_instance_id}" if f.strategy_instance_id is not None
                else "platform-wide"
            )
            print(
                f"  - {f.flag_type.value} [{f.scope.value}, {target}] by {f.activated_by}: {f.reason}",
                file=out,
            )
        return 0

    flag_type = RiskFlagType(args.flag_type)
    scope = RiskFlagScope(args.scope)
    if args.command == "engage":
        created = kill_switch.engage(
            reason=args.reason, activated_by=args.actor, flag_type=flag_type, scope=scope,
            account_id=args.account_id, strategy_instance_id=args.instance_id,
        )
        print("Kill switch engaged." if created else "Already engaged (no change).", file=out)
        return 0

    # disengage
    cleared = kill_switch.disengage(
        cleared_by=args.actor, flag_type=flag_type, scope=scope,
        account_id=args.account_id, strategy_instance_id=args.instance_id,
    )
    print(f"Cleared {cleared} flag(s).", file=out)
    return 0


def _build_kill_switch() -> KillSwitch:
    """Construct a KillSwitch bound to the configured database (DATABASE_URL).
    Lazy imports keep the CLI's import cost (and its DB connection) out of the
    unit-test path, which drives ``_run`` with a fake instead."""
    from algo.database.database import build_engine, load_database_settings
    from algo.database.session import build_session_factory
    from algo.risk.kill_switch import KillSwitch
    from algo.services.time_service import SystemTimeProvider

    engine = build_engine(load_database_settings())
    return KillSwitch(
        session_factory=build_session_factory(engine), time_provider=SystemTimeProvider()
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        kill_switch = _build_kill_switch()
        return _run(kill_switch, args)
    except Exception as exc:  # noqa: BLE001 -- turn any failure into a clean non-zero exit
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
