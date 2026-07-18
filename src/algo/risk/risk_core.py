"""risk_core.py: the concrete pre-trade risk gate.

Implements ``strategy_context.RiskGateway`` -- entry_logic.py and monitor.py
depend only on that Protocol, never on this class directly, so ``RiskCore``
can be swapped, extended, or replaced without touching either. Broker-agnostic
(depends only on the already-broker-agnostic ``BrokerBase``) and strategy-
agnostic (depends only on ``StrategyIdentity``, never on Strategy-1 specifics),
so it is reusable by any future strategy built on the same engine.

Consolidates, in one composable module, the pre-trade validation role the
original project skeleton sketched across ``risk_manager.py``,
``daily_loss_limit.py``, and ``margin_monitor.py``'s stub docstrings. This
module *reads* the control flags and daily-risk state; the *producer* side of
the kill switch now lives in ``kill_switch.py`` (the ``KillSwitch`` service +
operator CLI). The remaining ``risk/*.py`` stubs are left for a future, more
granular decomposition if the risk surface outgrows one module; nothing here
forecloses that.

Fail-fast by design: ``validate_entry`` runs a fixed, ordered sequence of named
checks -- cheapest and most deterministic first (pure time comparison), then
local database reads, then the most expensive and least deterministic last
(broker network calls) -- and stops at the first failure. Every check's
outcome, including the ones never reached because an earlier one already
failed (recorded as SKIPPED), is returned in the structured
``RiskValidationResult``, so a rejection is always traceable to one specific,
named reason instead of a single opaque boolean.

This module places no orders, contains no scheduler loop, and contains no
tick-monitoring loop -- it only answers "is this entry currently allowed",
a question strategy_engine code asks it, never the reverse.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, time
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING

from pydantic import BaseModel, ConfigDict, Field, model_validator

from algo.brokers.exceptions import BrokerError
from algo.common.enums import InstanceStatus, RiskFlagType
from algo.database.repositories.daily_risk_state_repository import DailyRiskStateRepository
from algo.database.repositories.exceptions import ConcurrentModificationError
from algo.database.repositories.position_repository import PositionRepository
from algo.database.repositories.risk_control_flag_repository import RiskControlFlagRepository
from algo.database.repositories.strategy_instance_repository import (
    StrategyInstanceRepository,
)
from algo.strategy_engine.strategy_context import RiskDecision

if TYPE_CHECKING:
    from sqlalchemy.orm import Session, sessionmaker

    from algo.brokers.broker_base import BrokerBase
    from algo.strategy_engine.strategy_context import StrategyIdentity, TimeProvider


# ---------------------------------------------------------------------------
# Structured results
# ---------------------------------------------------------------------------


class RiskCheckStatus(str, Enum):
    """Outcome of one individual pre-trade check."""

    PASSED = "PASSED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


@dataclass(frozen=True, slots=True)
class RiskCheckResult:
    """The outcome of one named pre-trade check.

    Always populated with a human-readable ``detail``, even on PASSED --
    a full audit trail of *why* an entry was approved is as valuable as why
    it was rejected, particularly for the once-a-day, money-moving decision
    this module exists to gate.
    """

    name: str
    status: RiskCheckStatus
    detail: str


@dataclass(frozen=True, slots=True)
class RiskValidationResult:
    """The complete, structured outcome of one ``validate_entry`` run: every
    check's individual result, in evaluation order, plus the overall verdict.
    """

    approved: bool
    checks: tuple[RiskCheckResult, ...]

    @property
    def failed_check(self) -> RiskCheckResult | None:
        """The check that caused rejection, or ``None`` if approved.
        Fail-fast evaluation means at most one FAILED entry ever appears."""
        for check in self.checks:
            if check.status is RiskCheckStatus.FAILED:
                return check
        return None

    def to_risk_decision(self) -> RiskDecision:
        """Adapt this rich result to the generic
        ``strategy_context.RiskGateway`` contract entry_logic.py consumes."""
        failed = self.failed_check
        reason = None if failed is None else f"{failed.name}: {failed.detail}"
        return RiskDecision(approved=self.approved, reason=reason)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class RiskCoreConfig(BaseModel):
    """Account/platform-level risk configuration -- deliberately not per-
    strategy (unlike ``Strategy1Config``): the same ``RiskCore`` instance can
    gate every strategy instance under an account, per "keep everything
    strategy-agnostic". Every field is required, no Python-level default --
    matching the platform's "no hidden defaults for anything that should come
    from config" rule.
    """

    model_config = ConfigDict(frozen=True)

    market_open_time: time = Field(
        description="IST time-of-day trading is considered open. An entry "
        "attempted before this is rejected regardless of any other check."
    )
    market_close_time: time = Field(
        description="IST time-of-day trading is considered closed."
    )
    max_daily_entries_per_account: int = Field(
        gt=0,
        description="Maximum number of new positions that may be opened "
        "across the whole account on one trading day, summed over every "
        "strategy instance under it -- an account-wide blast-radius cap, "
        "distinct from the single-instance duplicate-position check.",
    )
    legs_per_entry: int = Field(
        gt=0,
        description="Number of option legs one entry opens (2 for a "
        "straddle), used only to scale the available-margin estimate. Kept "
        "as config rather than a hardcoded constant so this module makes no "
        "silent assumption about any particular strategy's shape.",
    )
    margin_per_lot_by_instrument: dict[str, Decimal] = Field(
        description="Approximate margin (in account currency) blocked per "
        "lot, per instrument (e.g. {'NIFTY': 50000, 'SENSEX': 70000}). An "
        "estimate for the pre-trade sufficiency check -- not a real SPAN "
        "margin calculation, which would require a broker margin-calculator "
        "API this platform does not yet integrate.",
    )
    daily_loss_limit_by_account: Decimal | None = Field(
        description="Maximum tolerated (realized + unrealized) daily loss "
        "per account, as a positive number (a loss of more than this amount "
        "blocks further entries). Explicit null means the check is "
        "deliberately disabled, not silently skipped by omission.",
    )

    @model_validator(mode="after")
    def _check_market_hours_ordered(self) -> "RiskCoreConfig":
        if self.market_open_time >= self.market_close_time:
            raise ValueError(
                f"market_open_time ({self.market_open_time}) must be strictly "
                f"before market_close_time ({self.market_close_time})"
            )
        return self

    @model_validator(mode="after")
    def _check_margin_values_positive(self) -> "RiskCoreConfig":
        for instrument, value in self.margin_per_lot_by_instrument.items():
            if value <= 0:
                raise ValueError(
                    f"margin_per_lot_by_instrument[{instrument!r}] must be "
                    f"positive, got {value}"
                )
        return self

    @model_validator(mode="after")
    def _check_loss_limit_positive_if_set(self) -> "RiskCoreConfig":
        if self.daily_loss_limit_by_account is not None and self.daily_loss_limit_by_account <= 0:
            raise ValueError(
                "daily_loss_limit_by_account must be positive if set, got "
                f"{self.daily_loss_limit_by_account}"
            )
        return self


# ---------------------------------------------------------------------------
# RiskCore
# ---------------------------------------------------------------------------


class RiskCore:
    """Concrete ``RiskGateway``: runs every pre-trade check before an entry is
    approved, and exposes the cheap, independent halt check
    ``monitor.py`` polls on every tick/poll cycle.
    """

    def __init__(
        self,
        *,
        config: RiskCoreConfig,
        broker: BrokerBase,
        session_factory: sessionmaker[Session],
        time_provider: TimeProvider,
        logger: logging.Logger | None = None,
    ) -> None:
        self._config = config
        self._broker = broker
        self._session_factory = session_factory
        self._time = time_provider
        self._logger = logger if logger is not None else logging.getLogger("algo.risk.risk_core")

    # -- RiskGateway Protocol --------------------------------------------

    def is_halted(self, identity: StrategyIdentity) -> bool:
        """True if any active kill-switch/emergency-exit/freeze flag applies
        to this instance (globally, to its account, or to it specifically).

        Deliberately cheap (one indexed DB read, no broker call) and side-
        effect free, since ``monitor.py`` calls this on every tick/poll --
        unlike ``approve_entry``, this is a genuine hot path.
        """
        with self._session_factory() as session:
            flags = RiskControlFlagRepository(session).list_active_relevant_to(
                account_id=identity.account_id, strategy_instance_id=identity.instance_id
            )
        return bool(flags)

    def approve_entry(self, identity: StrategyIdentity, *, quantity: int) -> RiskDecision:
        """Protocol-facing entry point: run the full pre-trade validation and
        adapt it to the generic ``RiskDecision`` shape entry_logic.py expects.
        Prefer ``validate_entry`` directly when the richer, per-check result
        is useful (logging, testing, an operator-facing dashboard)."""
        return self.validate_entry(identity, quantity=quantity).to_risk_decision()

    # -- Structured validation --------------------------------------------

    def validate_entry(self, identity: StrategyIdentity, *, quantity: int) -> RiskValidationResult:
        """Run every pre-trade check in a fixed, fail-fast order and return
        the full structured result.

        Order (see the module docstring for the fail-fast rationale):
        trading_hours -> strategy_state -> kill_switch -> emergency_stop ->
        daily_loss_limit -> duplicate_position -> max_daily_entries ->
        broker_connectivity -> available_margin. Evaluation stops at the
        first FAILED check; every check after it is recorded as SKIPPED
        rather than silently omitted, so the returned result always shows
        the complete, intended check list regardless of where it stopped.
        """
        checks: list[RiskCheckResult] = []
        already_failed = False

        for name, check_fn in (
            ("trading_hours", lambda: self._check_trading_hours()),
            ("strategy_state", lambda: self._check_strategy_state(identity)),
            ("kill_switch", lambda: self._check_kill_switch(identity)),
            ("emergency_stop", lambda: self._check_emergency_stop(identity)),
            ("daily_loss_limit", lambda: self._check_daily_loss_limit(identity)),
            ("duplicate_position", lambda: self._check_duplicate_position(identity)),
            ("max_daily_entries", lambda: self._check_max_daily_entries(identity)),
            ("broker_connectivity", lambda: self._check_broker_connectivity()),
            ("available_margin", lambda: self._check_available_margin(identity, quantity)),
        ):
            if already_failed:
                checks.append(
                    RiskCheckResult(
                        name,
                        RiskCheckStatus.SKIPPED,
                        "not evaluated: an earlier check already failed (fail-fast)",
                    )
                )
                continue
            result = check_fn()
            checks.append(result)
            if result.status is RiskCheckStatus.FAILED:
                already_failed = True

        result = RiskValidationResult(approved=not already_failed, checks=tuple(checks))
        if not result.approved:
            failed = result.failed_check
            assert failed is not None
            self._logger.warning(
                "entry rejected for %s: %s (%s)", identity, failed.name, failed.detail
            )
        return result

    # -- Individual checks -------------------------------------------------
    # Each is self-contained (does its own DB/broker I/O) so it can be
    # exercised in isolation in tests without driving the full sequence.

    def _check_trading_hours(self) -> RiskCheckResult:
        now = self._time.now_ist().time()
        open_, close = self._config.market_open_time, self._config.market_close_time
        if open_ <= now <= close:
            return RiskCheckResult(
                "trading_hours", RiskCheckStatus.PASSED, f"{now} is within {open_}-{close}"
            )
        return RiskCheckResult(
            "trading_hours", RiskCheckStatus.FAILED, f"{now} is outside {open_}-{close}"
        )

    def _check_strategy_state(self, identity: StrategyIdentity) -> RiskCheckResult:
        with self._session_factory() as session:
            instance = StrategyInstanceRepository(session).get_by_id(identity.instance_id)
            if instance is None:
                return RiskCheckResult(
                    "strategy_state",
                    RiskCheckStatus.FAILED,
                    f"no StrategyInstance row found for id={identity.instance_id}",
                )
            if instance.status is not InstanceStatus.ACTIVE:
                return RiskCheckResult(
                    "strategy_state",
                    RiskCheckStatus.FAILED,
                    f"instance status is {instance.status.value}, not ACTIVE",
                )
            freeze_flags = [
                flag
                for flag in RiskControlFlagRepository(session).list_active_relevant_to(
                    account_id=identity.account_id, strategy_instance_id=identity.instance_id
                )
                if flag.flag_type is RiskFlagType.FREEZE
            ]
        if freeze_flags:
            return RiskCheckResult(
                "strategy_state",
                RiskCheckStatus.FAILED,
                f"{len(freeze_flags)} active FREEZE flag(s) present",
            )
        return RiskCheckResult(
            "strategy_state", RiskCheckStatus.PASSED, "instance is ACTIVE with no freeze flags"
        )

    def _check_kill_switch(self, identity: StrategyIdentity) -> RiskCheckResult:
        with self._session_factory() as session:
            flags = RiskControlFlagRepository(session).list_active_relevant_to(
                account_id=identity.account_id, strategy_instance_id=identity.instance_id
            )
        active = [f for f in flags if f.flag_type is RiskFlagType.KILL_SWITCH]
        if active:
            return RiskCheckResult(
                "kill_switch", RiskCheckStatus.FAILED, f"{len(active)} active KILL_SWITCH flag(s)"
            )
        return RiskCheckResult("kill_switch", RiskCheckStatus.PASSED, "no active kill-switch flag")

    def _check_emergency_stop(self, identity: StrategyIdentity) -> RiskCheckResult:
        with self._session_factory() as session:
            flags = RiskControlFlagRepository(session).list_active_relevant_to(
                account_id=identity.account_id, strategy_instance_id=identity.instance_id
            )
        active = [f for f in flags if f.flag_type is RiskFlagType.EMERGENCY_EXIT]
        if active:
            return RiskCheckResult(
                "emergency_stop",
                RiskCheckStatus.FAILED,
                f"{len(active)} active EMERGENCY_EXIT flag(s)",
            )
        return RiskCheckResult(
            "emergency_stop", RiskCheckStatus.PASSED, "no active emergency-stop flag"
        )

    def _check_daily_loss_limit(self, identity: StrategyIdentity) -> RiskCheckResult:
        """Enforce the account's configured daily loss limit (resolves the
        producer gap that previously left this check inert).

        The day's realized P&L is *recomputed from the source-of-truth Position
        rows on every check* (``realized_pnl_for_account_on_date``) rather than
        read from a separately-maintained running total, so it is inherently
        correct across restarts and can never drift. That figure is written back
        onto today's ``DailyRiskState`` row (for monitoring/reporting), and the
        moment it crosses ``-loss_limit`` the row's ``breached`` flag is latched
        True. The latch is sticky and persisted: once an account has breached
        for the day it stays blocked even if a later position's P&L would pull
        the total back above the limit -- hitting the daily loss limit ends the
        day, by design.

        The limit is snapshotted onto the row on first touch, so a mid-day
        config edit cannot retroactively change what already happened (the same
        rule Position's own entry-time config snapshot follows).

        Concurrency: the *decision* is computed by a read-only query, so it can
        never lose an optimistic-lock race with another instrument entering
        under the same account at the same instant (a real possibility now that
        the scheduler dispatches coincident triggers concurrently). Only the
        best-effort *persist* of the recomputed figure touches the shared row,
        and that tolerates a concurrent-modification conflict rather than
        failing the check -- see ``_persist_daily_risk``.
        """
        limit = self._config.daily_loss_limit_by_account
        if limit is None:
            return RiskCheckResult(
                "daily_loss_limit", RiskCheckStatus.PASSED, "no daily loss limit configured"
            )
        trade_date = self._time.today()

        # 1. Authoritative, read-only decision (never writes -> never races).
        with self._session_factory() as session:
            existing = DailyRiskStateRepository(session).get_portfolio_row(
                identity.account_id, trade_date
            )
            already_breached = bool(existing is not None and existing.breached)
            unrealized = existing.unrealized_pnl if existing is not None else Decimal("0")
            # Compare against the row's snapshotted limit once it exists, else
            # today's config value (which the persist step snapshots on first
            # touch) -- preserving the "config edit is not retroactive" rule.
            effective_limit = existing.loss_limit if existing is not None else limit
            realized = PositionRepository(session).realized_pnl_for_account_on_date(
                identity.account_id, trade_date
            )
        total_pnl = realized + unrealized
        crossed = total_pnl <= -effective_limit
        breached = already_breached or crossed

        # 2. Best-effort persist for monitoring + the breach latch.
        self._persist_daily_risk(
            identity.account_id, trade_date,
            config_limit=limit, realized=realized, unrealized=unrealized, latch_breach=crossed,
        )

        if breached:
            return RiskCheckResult(
                "daily_loss_limit",
                RiskCheckStatus.FAILED,
                f"daily realized P&L {total_pnl} breaches limit of -{effective_limit}",
            )
        return RiskCheckResult(
            "daily_loss_limit",
            RiskCheckStatus.PASSED,
            f"daily realized P&L {total_pnl} within limit of -{effective_limit}",
        )

    def _persist_daily_risk(
        self,
        account_id: int,
        trade_date: date,
        *,
        config_limit: Decimal,
        realized: Decimal,
        unrealized: Decimal,
        latch_breach: bool,
    ) -> None:
        """Write the recomputed daily P&L (and latch the breach flag) onto the
        account's ``DailyRiskState`` row, tolerating a concurrent-modification
        conflict.

        This is deliberately best-effort: the loss-limit *decision* is made
        read-only by the caller from the source-of-truth position rows, so if a
        coincident entry under the same account wins the optimistic-lock race on
        this shared row, skipping the write here changes nothing about the
        decision -- the figure is corrected on the next check, and the breach is
        re-latched then. This is what makes concurrent same-account entries safe
        (they would otherwise freeze on a benign ``ConcurrentModificationError``).
        """
        session = self._session_factory()
        try:
            repo = DailyRiskStateRepository(session)
            row, _created = repo.get_or_create_portfolio_row(
                account_id=account_id, trade_date=trade_date, loss_limit=config_limit
            )
            repo.update_pnl(row, realized_pnl=realized, unrealized_pnl=unrealized)
            if latch_breach and not row.breached:
                repo.mark_breached(row, breached_at=self._time.now())
            session.commit()
        except ConcurrentModificationError:
            session.rollback()
            self._logger.info(
                "daily risk row for account %d was concurrently updated; skipping this "
                "persist (the loss-limit decision was already computed read-only)", account_id,
            )
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _check_duplicate_position(self, identity: StrategyIdentity) -> RiskCheckResult:
        trade_date = self._time.today()
        with self._session_factory() as session:
            existing = PositionRepository(session).get_by_instance_and_date(
                identity.instance_id, trade_date
            )
        if existing is not None:
            return RiskCheckResult(
                "duplicate_position",
                RiskCheckStatus.FAILED,
                f"position {existing.id} already exists for {trade_date} "
                f"(state={existing.state.value})",
            )
        return RiskCheckResult(
            "duplicate_position", RiskCheckStatus.PASSED, f"no existing position for {trade_date}"
        )

    def _check_max_daily_entries(self, identity: StrategyIdentity) -> RiskCheckResult:
        trade_date = self._time.today()
        with self._session_factory() as session:
            count = PositionRepository(session).count_for_account_on_date(
                identity.account_id, trade_date
            )
        limit = self._config.max_daily_entries_per_account
        if count >= limit:
            return RiskCheckResult(
                "max_daily_entries",
                RiskCheckStatus.FAILED,
                f"{count} entries already opened today account-wide, limit is {limit}",
            )
        return RiskCheckResult(
            "max_daily_entries",
            RiskCheckStatus.PASSED,
            f"{count}/{limit} entries opened today account-wide",
        )

    def _check_broker_connectivity(self) -> RiskCheckResult:
        try:
            status = self._broker.health_check()
        except BrokerError as exc:
            return RiskCheckResult(
                "broker_connectivity", RiskCheckStatus.FAILED, f"health_check raised: {exc}"
            )
        if not status.healthy:
            return RiskCheckResult(
                "broker_connectivity",
                RiskCheckStatus.FAILED,
                f"broker reports unhealthy: {status.detail}",
            )
        return RiskCheckResult(
            "broker_connectivity",
            RiskCheckStatus.PASSED,
            f"broker healthy (latency_ms={status.latency_ms})",
        )

    def _check_available_margin(
        self, identity: StrategyIdentity, quantity: int
    ) -> RiskCheckResult:
        per_lot = self._config.margin_per_lot_by_instrument.get(identity.instrument)
        if per_lot is None:
            return RiskCheckResult(
                "available_margin",
                RiskCheckStatus.FAILED,
                f"no margin_per_lot_by_instrument configured for {identity.instrument!r}",
            )
        required = per_lot * quantity * self._config.legs_per_entry
        try:
            margins = self._broker.get_margins()
        except BrokerError as exc:
            return RiskCheckResult(
                "available_margin", RiskCheckStatus.FAILED, f"get_margins raised: {exc}"
            )
        if margins.available_cash < required:
            return RiskCheckResult(
                "available_margin",
                RiskCheckStatus.FAILED,
                f"available {margins.available_cash} < estimated required {required} "
                f"({quantity} lots x {self._config.legs_per_entry} legs x {per_lot}/lot)",
            )
        return RiskCheckResult(
            "available_margin",
            RiskCheckStatus.PASSED,
            f"available {margins.available_cash} >= estimated required {required}",
        )
