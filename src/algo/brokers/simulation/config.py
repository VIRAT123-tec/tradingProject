"""Tunable behavior for SimulationBroker.

Every probability/latency here is configuration the caller supplies (from
brokers.yaml in production use, or constructed directly in a test) -- never a
constant baked into simulation_broker.py.
"""

from __future__ import annotations

from decimal import Decimal
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field


class FillOutcome(str, Enum):
    """What the matching engine decided will happen to a placed order, fixed
    once at placement time (the one point where randomness is consulted) and
    merely executed over time afterward -- so replaying the same seed and
    call sequence always produces the same sequence of outcomes, even though
    *when* each step fires depends on the clock.
    """

    FULL_FILL = "FULL_FILL"
    PARTIAL_THEN_FILL = "PARTIAL_THEN_FILL"
    REJECTED = "REJECTED"


class SimulationConfig(BaseModel):
    """All tunable knobs for one SimulationBroker instance.

    synchronous=True resolves every order fully inline within place_order/
    modify_order/cancel_order -- no background thread, no real waiting.
    Configured latencies are ignored in this mode (there is nothing to wait
    on); use it for fast strategy-logic tests that don't care about the
    ack-then-fill timing window itself.

    synchronous=False (the paper-trading default) runs a background matching
    thread that resolves orders after the configured latencies elapse, so
    callers observe the same PENDING -> OPEN -> COMPLETE progression a real
    broker produces, including the window where a placed order is not yet
    filled -- this is the mode that can actually exercise the
    SUBMITTED_UNCONFIRMED crash-recovery path.
    """

    model_config = ConfigDict(frozen=True)

    synchronous: bool = False

    ack_latency_seconds: float = Field(default=0.05, ge=0)
    fill_latency_seconds: float = Field(default=0.2, ge=0)
    matching_tick_seconds: float = Field(default=0.02, gt=0)

    auth_should_fail: bool = False
    connection_failure_probability: float = Field(default=0.0, ge=0, le=1)
    ack_timeout_probability: float = Field(default=0.0, ge=0, le=1)
    rejection_probability: float = Field(default=0.0, ge=0, le=1)
    partial_fill_probability: float = Field(default=0.0, ge=0, le=1)
    min_partial_fill_steps: int = Field(default=2, ge=2)
    max_partial_fill_steps: int = Field(default=3, ge=2)
    websocket_connect_failure_probability: float = Field(default=0.0, ge=0, le=1)

    initial_cash: Decimal = Decimal("1000000")
    margin_per_lot: Decimal = Field(default=Decimal("50000"), gt=0)

    @property
    def resolved_max_partial_fill_steps(self) -> int:
        return max(self.min_partial_fill_steps, self.max_partial_fill_steps)
