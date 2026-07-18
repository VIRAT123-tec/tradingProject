"""Strategy: the single instrument-agnostic contract every concrete strategy
implements, plus the small value types that contract exchanges.

The engine (``StrategyRunner``) drives an instance purely through the abstract
methods here -- it never knows whether it is running a short straddle, a
directional trade, or anything else. All instrument-specific behavior arrives
through the injected ``StrategyContext`` (config, broker, market data), never
through conditionals in engine code.

Every hook is abstract on purpose. For a system that will move real capital,
"you must consciously decide what happens on recovery / on shutdown / on a
missed trigger" is safer than inheriting a silent no-op default.
"""

from __future__ import annotations

import abc
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, time
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pydantic import BaseModel

    from algo.strategy_engine.strategy_context import StrategyContext, Tick


class TriggerCatchUpPolicy(str, Enum):
    """What to do with a scheduled trigger whose time-of-day has already
    passed at the moment the process starts (e.g. a restart at 09:25 when the
    'entry' trigger was 09:20).

    SKIP is the safe default: a missed entry window is a deliberate no-trade,
    not something to fire late into a moved market. FIRE_ON_STARTUP exists for
    triggers where catching up is genuinely correct (e.g. a 'hard cutoff' that
    must still force an exit even if we restarted past it).
    """

    SKIP = "SKIP"
    FIRE_ON_STARTUP = "FIRE_ON_STARTUP"


@dataclass(frozen=True, slots=True)
class TimeTrigger:
    """A named time-of-day trigger the strategy wants the scheduler to fire.

    The strategy derives these from its own (already-validated) config -- e.g.
    an 'entry' trigger at the configured entry time and a 'cutoff' trigger at
    the configured hard-exit time. The scheduler treats them opaquely: it
    knows only "fire a trigger named X at time-of-day T", never what X means.
    ``trigger_time`` is a wall-clock IST time-of-day; the scheduler is
    responsible for resolving it against the trading calendar.
    """

    name: str
    trigger_time: time
    catch_up: TriggerCatchUpPolicy = TriggerCatchUpPolicy.SKIP

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("TimeTrigger.name must be a non-empty string")


@dataclass(frozen=True, slots=True)
class StrategyHealth:
    """A strategy-agnostic health snapshot for monitoring/heartbeat.

    ``state`` is a free-form string (e.g. the strategy's current state-machine
    state) meaningful for logs and dashboards but never interpreted by the
    engine.
    """

    healthy: bool
    state: str
    checked_at: datetime
    detail: str | None = None


class Strategy(abc.ABC):
    """Abstract base for every concrete strategy.

    Constructed with its fully-wired ``StrategyContext`` (constructor
    injection), so every hook can reach dependencies through ``self.context``
    without taking them as parameters. Subclasses must not perform I/O or
    place orders in ``__init__`` -- construction only stores the context;
    ``initialize()`` / ``recover()`` are where a live instance comes up.
    """

    def __init__(self, context: StrategyContext) -> None:
        self._context = context

    @property
    def context(self) -> StrategyContext:
        """The injected dependency bundle (identity, config, broker, market
        data, risk, time, logger)."""
        return self._context

    # -- Configuration -------------------------------------------------

    @classmethod
    @abc.abstractmethod
    def config_schema(cls) -> type[BaseModel]:
        """Return the Pydantic model this strategy's per-instrument YAML is
        validated into.

        A classmethod because it must be known *before* an instance exists:
        ``instance_factory`` loads and validates config against this type,
        then passes the resulting model into the context used to build the
        instance.
        """
        raise NotImplementedError

    # -- Scheduling ----------------------------------------------------

    @abc.abstractmethod
    def scheduled_triggers(self) -> Sequence[TimeTrigger]:
        """The time-of-day triggers this instance wants fired, derived from
        its config. Read once by the runner/scheduler at start; the scheduler
        then calls ``on_time_trigger`` when each is due."""
        raise NotImplementedError

    # -- Lifecycle -----------------------------------------------------

    @abc.abstractmethod
    def initialize(self) -> None:
        """One-time setup for a *fresh* instance with no prior persisted
        state (e.g. subscribe to market data). Called by the runner at start
        when recovery finds nothing to resume."""
        raise NotImplementedError

    @abc.abstractmethod
    def recover(self) -> None:
        """Reconstruct in-memory view from the database on process start.

        The database is the source of truth: an instance that finds an
        already-open position must resume monitoring it rather than assume a
        clean IDLE slate. Implementations that find no prior state should be
        equivalent to a fresh ``initialize()``. Called by the runner before
        it accepts any triggers or ticks.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def on_time_trigger(self, trigger_name: str) -> None:
        """Handle a scheduled trigger (identified by the name the strategy
        itself declared in ``scheduled_triggers``) becoming due."""
        raise NotImplementedError

    @abc.abstractmethod
    def on_market_tick(self, tick: Tick) -> None:
        """Handle one market-data tick. May be called at high frequency; the
        runner guarantees calls are serialized with respect to time triggers
        for this instance, so implementations need no locking of their own."""
        raise NotImplementedError

    def on_monitor_cycle(self) -> None:
        """Periodic 'check the open position now' cadence, driven by the
        monitoring scheduler between scheduled time triggers.

        This is the guaranteed, feed-independent safety-net that evaluates the
        price-based exits (stop-loss / target) and the price-independent ones
        (kill-switch / hard cutoff) on a fixed cadence, using a polling read
        of the broker when the live tick feed is stale or absent. Without it,
        a strategy that relies only on pushed ticks would never evaluate an
        exit if the websocket were quiet.

        Deliberately concrete with a **no-op default**, unlike ``recover`` /
        ``on_shutdown`` (which are abstract so each strategy must consciously
        decide). The honest default for a strategy that is not currently
        watching a position is genuinely "do nothing", so forcing every
        strategy to implement it would add ceremony without safety. A strategy
        that manages an open position (e.g. Strategy-1) overrides this to run
        its monitor. Serialized with triggers/ticks by the runner's lock, so
        implementations need no locking of their own.
        """

    @abc.abstractmethod
    def on_shutdown(self) -> None:
        """Release resources for a graceful stop (e.g. unsubscribe from market
        data). Must not assume the position is closed -- shutting the process
        down does not flatten open trades; it only stops this instance's
        event handling."""
        raise NotImplementedError

    # -- Introspection -------------------------------------------------

    @abc.abstractmethod
    def health(self) -> StrategyHealth:
        """Return a current health snapshot for monitoring. Must be cheap and
        must not raise; the runner treats an exception here as unhealthy."""
        raise NotImplementedError
