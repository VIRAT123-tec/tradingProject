"""Pydantic config schema for Strategy-1's per-instrument YAML
(configs/strategies/strategy_1/{nifty,sensex}.yaml), loaded and validated by
parameter_loader.py at startup -- a bad or missing value here fails at process
start, not at 09:20 when an order is about to be placed.

Every field is required, with no Python-level default: if a value should come
from config, it must be present in the YAML explicitly, never silently assumed
by code. ``skip_on_expiry_day`` in particular was left as an open policy
question earlier in this project -- rather than guessing an answer in code,
every instrument's YAML must state it explicitly. The same rule applies to
``retry.order_timeout_seconds``: its explicit value is ``null`` for "no
override", never an implicit Python default.

This module contains no business logic -- no order placement, no premium math,
no state transitions. Its only behavior is structural validation: type
coercion, per-field bounds, and the handful of cross-field sanity checks
(entry before cutoff, polling tighter than the monitoring safety net) that
catch an internally-contradictory config at load time rather than at runtime.
"""

from __future__ import annotations

from datetime import time
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from algo.common.enums import ProductType


class RetrySettings(BaseModel):
    """Timeout/retry tuning for broker order placement and fill confirmation,
    shared by entry_logic.py and exit_logic.py.

    Every field here corresponds exactly to an existing constructor parameter
    on ``EntryLogic``/``ExitLogic`` (previously left at a hardcoded Python
    default) -- this model exists so those values are config-driven per
    instrument instead, per the platform's "nothing instrument- or
    strategy-specific is hardcoded" rule. ``close_retry_attempts``/
    ``close_retry_delay_seconds`` apply to exit only (entry never blindly
    retries a placement -- see entry_logic.py's ambiguous-outcome contract);
    they are still required here rather than defaulted, so every instrument's
    YAML states a deliberate value even though entry_logic ignores them.
    """

    model_config = ConfigDict(frozen=True)

    order_timeout_seconds: float | None = Field(
        gt=0,
        description="Per-call timeout (seconds) passed to broker calls "
        "(place_order/get_order/...). Explicit null means 'no caller-side "
        "override -- use the broker implementation's own configured default', "
        "a deliberate config choice, not an omitted one; a non-null value must "
        "be positive.",
    )
    fill_confirmation_attempts: int = Field(
        gt=0,
        description="Max number of get_order polls while waiting for a placed "
        "order to reach a terminal status before treating it as ambiguous.",
    )
    fill_confirmation_delay_seconds: float = Field(
        gt=0, description="Delay between fill-confirmation polls, in seconds."
    )
    close_retry_attempts: int = Field(
        gt=0,
        description="Max attempts to (re)send an exit close order after a "
        "never-sent (connection/rate-limit) failure -- never applies to a "
        "rejection or a timeout, both of which are handled without retrying.",
    )
    close_retry_delay_seconds: float = Field(
        gt=0, description="Delay between close-order retry attempts, in seconds."
    )


class Strategy1Config(BaseModel):
    """Per-instrument configuration for Strategy-1 (the short-straddle
    strategy) -- one validated instance per instrument YAML file (e.g. one for
    Nifty, one for Sensex), returned by ``Strategy1.config_schema()`` per the
    strategy_engine contract.
    """

    model_config = ConfigDict(frozen=True)

    entry_time: time = Field(
        description="IST time-of-day the entry trigger fires (e.g. 09:20:00)."
    )
    hard_cutoff_time: time = Field(
        description="IST time-of-day the hard exit cutoff fires, forcing an "
        "exit regardless of P&L (e.g. 15:15:00) -- this is the config's "
        "'exit time', consumed by exit_logic.evaluate_exit as the highest-"
        "priority trigger."
    )
    target_pct: Decimal = Field(
        gt=Decimal("0"),
        lt=Decimal("1"),
        description="Fractional target: exit when combined premium falls to "
        "entry_premium * (1 - target_pct). E.g. 0.10 for a 10% target.",
    )
    sl_pct: Decimal = Field(
        gt=Decimal("0"),
        lt=Decimal("1"),
        description="Fractional stoploss: exit when combined premium rises to "
        "entry_premium * (1 + sl_pct). E.g. 0.10 for a 10% stoploss.",
    )
    lots: int = Field(
        gt=0, description="Number of lots per leg; order quantity = lots * lot_size."
    )
    product_type: ProductType = Field(
        description="Broker product for entry/exit orders (INTRADAY or NORMAL). "
        "For a same-day-squared-off straddle this is expected to be INTRADAY, "
        "but it is required config with no default -- the MIS/NRML choice and "
        "its interaction with the broker's own auto-square-off must be stated "
        "explicitly per instrument, never assumed by code."
    )
    skip_on_expiry_day: bool = Field(
        description="Whether to skip entry entirely when today is this "
        "instrument's own weekly expiry day (0DTE). No default: every "
        "instrument's YAML must state this explicitly."
    )
    monitoring_interval_seconds: float = Field(
        gt=0,
        description="How often, in seconds, the scheduler should invoke "
        "PositionMonitor.poll_and_check() as the routine safety-net cadence "
        "while a position is open -- this is what enforces the hard cutoff / "
        "kill-switch triggers even when the websocket feed is healthy and no "
        "fresh tick happens to be flowing at that instant.",
    )
    polling_interval_seconds: float = Field(
        gt=0,
        description="How often, in seconds, the scheduler should invoke "
        "PositionMonitor.poll_and_check() specifically while the websocket "
        "feed is degraded/stale, compensating for lost push-based ticks. "
        "Expected to be tighter (smaller) than monitoring_interval_seconds, "
        "since it is covering for a known-worse data source.",
    )
    retry: RetrySettings = Field(
        description="Broker call timeout and retry tuning for entry/exit order "
        "placement and fill confirmation -- see RetrySettings."
    )

    @model_validator(mode="after")
    def _check_entry_before_cutoff(self) -> "Strategy1Config":
        if self.entry_time >= self.hard_cutoff_time:
            raise ValueError(
                f"entry_time ({self.entry_time}) must be strictly before "
                f"hard_cutoff_time ({self.hard_cutoff_time})"
            )
        return self

    @model_validator(mode="after")
    def _check_polling_tighter_than_monitoring(self) -> "Strategy1Config":
        if self.polling_interval_seconds > self.monitoring_interval_seconds:
            raise ValueError(
                f"polling_interval_seconds ({self.polling_interval_seconds}) must not "
                f"exceed monitoring_interval_seconds ({self.monitoring_interval_seconds}) "
                "-- the degraded-feed fallback cadence should never be looser than the "
                "healthy-feed safety-net cadence"
            )
        return self
