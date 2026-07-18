"""KillSwitch: the producer half of the platform's kill-switch / emergency-stop
mechanism (resolves review finding H2).

Background: ``risk_core.py`` already *reads* control flags -- it blocks new
entries while a KILL_SWITCH / EMERGENCY_EXIT / FREEZE flag relevant to an
instance is active, and ``monitor.py`` exits an open position when
``RiskGateway.is_halted`` reports one. What was missing was any way to *set*
one. This class is that producer, plus the small, safe scope logic around it.

How the pieces fit (nothing here places or closes orders):

* **Prevent new entries** -- an active flag makes ``RiskCore``'s ``kill_switch``
  / ``emergency_stop`` checks FAIL, so ``validate_entry`` rejects the entry.
* **Handle open positions safely** -- ``is_halted`` returns True while a flag is
  active, and the monitoring heartbeat's exit evaluation treats a halted state
  as an exit trigger (the approved exit priority), so open positions are
  flattened by the normal, tested exit path rather than by any bespoke logic
  here.
* **Persist across restarts** -- a flag is a durable ``risk_control_flags`` row,
  so a kill switch engaged before a restart is still in force after it.

Two ways to engage it:

* **In-process** -- the running platform holds a ``KillSwitch`` (wired by the
  dependency container) and can engage it programmatically.
* **Out-of-process** -- an operator runs the ``algo.killswitch`` CLI, which
  writes the flag to the same database the running platform polls. The running
  platform picks it up on its next ``is_halted`` check (every monitoring
  heartbeat, ~every couple of seconds) -- the "big red button" that works
  against an already-running process.

Thread-safe and idempotent: each call is its own committed unit of work, and
``engage`` will not create a duplicate flag if an equivalent one is already
active, so engaging twice (or a restart re-engaging) leaves exactly one flag.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING

from algo.common.enums import RiskFlagScope, RiskFlagType
from algo.database.repositories.risk_control_flag_repository import RiskControlFlagRepository
from algo.database.session import unit_of_work

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.orm import Session, sessionmaker

    from algo.database.models.risk_control_flag import RiskControlFlag
    from algo.strategy_engine.strategy_context import TimeProvider


class KillSwitch:
    """Engages, clears, and reports the platform's risk-control flags."""

    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        time_provider: TimeProvider,
        logger: logging.Logger | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._time = time_provider
        self._logger = logger if logger is not None else logging.getLogger("algo.risk.kill_switch")

    # -- Commands ------------------------------------------------------

    def engage(
        self,
        *,
        reason: str,
        activated_by: str,
        flag_type: RiskFlagType = RiskFlagType.KILL_SWITCH,
        scope: RiskFlagScope = RiskFlagScope.GLOBAL,
        account_id: int | None = None,
        strategy_instance_id: int | None = None,
    ) -> bool:
        """Activate a control flag.

        Defaults to a GLOBAL KILL_SWITCH -- the application-wide "stop
        everything" button. Narrower scopes (a single account or instance) and
        other flag types (EMERGENCY_EXIT, FREEZE) are available for finer
        control.

        Idempotent: if an equivalent flag (same type/scope/target) is already
        active, this is a no-op and returns ``False``; it returns ``True`` when
        a new flag was created. So engaging twice, or a restart re-engaging,
        leaves exactly one active flag rather than a stack of duplicates.
        """
        self._validate_scope_target(scope, account_id, strategy_instance_id)
        with self._unit_of_work() as session:
            repo = RiskControlFlagRepository(session)
            if self._find_matching_active(repo, flag_type, scope, account_id, strategy_instance_id) is not None:
                self._logger.info(
                    "kill switch already engaged (%s/%s); no-op", flag_type.value, scope.value
                )
                return False
            repo.activate(
                flag_type=flag_type,
                scope=scope,
                reason=reason,
                activated_by=activated_by,
                activated_at=self._time.now(),
                account_id=account_id,
                strategy_instance_id=strategy_instance_id,
            )
        self._logger.critical(
            "KILL SWITCH ENGAGED: %s/%s by %s (%s)", flag_type.value, scope.value, activated_by, reason
        )
        return True

    def disengage(
        self,
        *,
        cleared_by: str,
        flag_type: RiskFlagType = RiskFlagType.KILL_SWITCH,
        scope: RiskFlagScope = RiskFlagScope.GLOBAL,
        account_id: int | None = None,
        strategy_instance_id: int | None = None,
    ) -> int:
        """Clear every active flag matching the given type/scope/target.
        Returns the number of flags cleared (0 if none were active). Clearing
        is deliberately an explicit operator action -- the caller has confirmed
        it is safe to resume."""
        self._validate_scope_target(scope, account_id, strategy_instance_id)
        cleared = 0
        with self._unit_of_work() as session:
            repo = RiskControlFlagRepository(session)
            for flag in repo.list_active():
                if self._matches(flag, flag_type, scope, account_id, strategy_instance_id):
                    repo.clear(flag, cleared_by=cleared_by, cleared_at=self._time.now())
                    cleared += 1
        if cleared:
            self._logger.warning(
                "kill switch disengaged: cleared %d %s/%s flag(s) by %s",
                cleared, flag_type.value, scope.value, cleared_by,
            )
        return cleared

    # -- Queries -------------------------------------------------------

    def is_engaged(
        self,
        *,
        flag_type: RiskFlagType | None = None,
        scope: RiskFlagScope | None = None,
    ) -> bool:
        """Whether any active flag matches. With no arguments, reports whether
        *any* control flag at all is active (the broadest "are we halted"
        question). ``flag_type``/``scope`` narrow it."""
        with self._session_factory() as session:
            for flag in RiskControlFlagRepository(session).list_active():
                if flag_type is not None and flag.flag_type is not flag_type:
                    continue
                if scope is not None and flag.scope is not scope:
                    continue
                return True
        return False

    def list_active(self) -> list[RiskControlFlag]:
        """Every currently active control flag, for operator status output."""
        with self._session_factory() as session:
            flags = RiskControlFlagRepository(session).list_active()
            # Touch the attributes the CLI prints while the session is open, so
            # they remain available after it closes.
            for f in flags:
                _ = (f.flag_type, f.scope, f.account_id, f.strategy_instance_id, f.reason, f.activated_by)
            return flags

    # -- Internal ------------------------------------------------------

    @staticmethod
    def _validate_scope_target(
        scope: RiskFlagScope, account_id: int | None, strategy_instance_id: int | None
    ) -> None:
        """Fail fast on a structurally invalid scope/target combination, with a
        clear message, rather than letting it reach the model's CHECK
        constraint as an opaque IntegrityError."""
        if scope is RiskFlagScope.GLOBAL and (account_id is not None or strategy_instance_id is not None):
            raise ValueError("GLOBAL scope must not set account_id or strategy_instance_id")
        if scope is RiskFlagScope.ACCOUNT and (account_id is None or strategy_instance_id is not None):
            raise ValueError("ACCOUNT scope requires account_id and must not set strategy_instance_id")
        if scope is RiskFlagScope.STRATEGY_INSTANCE and strategy_instance_id is None:
            raise ValueError("STRATEGY_INSTANCE scope requires strategy_instance_id")

    @staticmethod
    def _matches(
        flag: RiskControlFlag,
        flag_type: RiskFlagType,
        scope: RiskFlagScope,
        account_id: int | None,
        strategy_instance_id: int | None,
    ) -> bool:
        return (
            flag.flag_type is flag_type
            and flag.scope is scope
            and flag.account_id == account_id
            and flag.strategy_instance_id == strategy_instance_id
        )

    def _find_matching_active(
        self,
        repo: RiskControlFlagRepository,
        flag_type: RiskFlagType,
        scope: RiskFlagScope,
        account_id: int | None,
        strategy_instance_id: int | None,
    ) -> RiskControlFlag | None:
        for flag in repo.list_active():
            if self._matches(flag, flag_type, scope, account_id, strategy_instance_id):
                return flag
        return None

    @contextmanager
    def _unit_of_work(self) -> Iterator[Session]:
        """One committed transaction against the injected factory, so a flag
        write is durable the moment the call returns (see
        ``database.session.unit_of_work``)."""
        with unit_of_work(self._session_factory) as session:
            yield session
