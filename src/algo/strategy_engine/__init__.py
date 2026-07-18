"""Strategy engine: instrument-agnostic strategy base/runner/registry machinery,
plus concrete strategies under strategy_engine/strategies/.

The generic framework (this package's top level) contains no trading logic: it
turns a registered ``Strategy`` subclass + an instrument config + a persistent
identity into a recoverable, schedulable, gracefully-stoppable running unit.
"""

from algo.strategy_engine.instance_factory import InstanceFactory
from algo.strategy_engine.parameter_loader import ParameterLoader
from algo.strategy_engine.strategy_base import (
    Strategy,
    StrategyHealth,
    TimeTrigger,
    TriggerCatchUpPolicy,
)
from algo.strategy_engine.strategy_context import (
    MarketDataGateway,
    RiskDecision,
    RiskGateway,
    StrategyContext,
    StrategyIdentity,
    Tick,
    TimeProvider,
)
from algo.strategy_engine.strategy_registry import (
    StrategyAlreadyRegisteredError,
    StrategyNotRegisteredError,
    StrategyRegistry,
    StrategyRegistryError,
    default_registry,
    register_strategy,
)
from algo.strategy_engine.strategy_runner import RunnerStatus, StrategyRunner

__all__ = [
    "Strategy",
    "StrategyHealth",
    "TimeTrigger",
    "TriggerCatchUpPolicy",
    "StrategyContext",
    "StrategyIdentity",
    "Tick",
    "MarketDataGateway",
    "RiskGateway",
    "RiskDecision",
    "TimeProvider",
    "StrategyRegistry",
    "StrategyRegistryError",
    "StrategyAlreadyRegisteredError",
    "StrategyNotRegisteredError",
    "default_registry",
    "register_strategy",
    "ParameterLoader",
    "StrategyRunner",
    "RunnerStatus",
    "InstanceFactory",
]
