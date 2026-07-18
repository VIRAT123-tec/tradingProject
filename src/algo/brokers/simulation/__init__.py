"""Fake/simulation broker implementing broker_base.py's interface identically
to kite/, so start_paper.py exercises the same strategy code as
start_live.py."""

from algo.brokers.simulation.clock import Clock, ManualClock, SystemClock
from algo.brokers.simulation.config import FillOutcome, SimulationConfig
from algo.brokers.simulation.instrument_catalog import InstrumentCatalog
from algo.brokers.simulation.price_source import (
    PriceSource,
    RandomWalkPriceSource,
    StaticPriceSource,
)
from algo.brokers.simulation.simulation_broker import SimulationBroker

__all__ = [
    "SimulationBroker",
    "SimulationConfig",
    "FillOutcome",
    "InstrumentCatalog",
    "PriceSource",
    "StaticPriceSource",
    "RandomWalkPriceSource",
    "Clock",
    "SystemClock",
    "ManualClock",
]
