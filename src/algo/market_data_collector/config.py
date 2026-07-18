"""CollectorConfig: the validated schema for configs/collector.yaml.

Every knob the collector has lives here -- ATM window, tick mode, writer
batching/queue/overflow, market-hours, metrics cadence, and Timescale policy --
so nothing is hardcoded in code. Loaded and validated once at startup; a bad
value fails the process at boot.
"""

from __future__ import annotations

from datetime import time
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator


class WriterConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    batch_size: int = Field(default=5000, gt=0)
    flush_interval_ms: int = Field(default=1000, gt=0)
    queue_max: int = Field(default=200_000, gt=0)
    overflow_policy: Literal["drop_oldest", "drop_newest"] = "drop_oldest"
    use_copy: bool = True
    db_retry_base_seconds: float = Field(default=0.5, gt=0)
    db_retry_max_seconds: float = Field(default=10.0, gt=0)


class MarketHoursConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    connect: time = time(9, 14)
    start: time = time(9, 15)
    stop_subscribe: time = time(15, 30)
    flush: time = time(15, 31)
    disconnect: time = time(15, 32)
    idle_poll_seconds: float = Field(default=30.0, gt=0)

    @field_validator("connect", "start", "stop_subscribe", "flush", "disconnect", mode="before")
    @classmethod
    def _parse_hhmm(cls, v: object) -> object:
        # Accept "HH:MM" / "HH:MM:SS" strings from YAML.
        if isinstance(v, str):
            parts = [int(p) for p in v.split(":")]
            return time(*parts)
        return v


class MetricsConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    interval_seconds: float = Field(default=60.0, gt=0)


class TimescaleConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    chunk_interval: str = "1 day"
    compression_after: str | None = "7 days"
    retention: str | None = None  # None = keep forever
    continuous_aggregates: list[str] = Field(default_factory=lambda: ["1 minute", "5 minutes"])


class CollectorConfig(BaseModel):
    """Top-level collector configuration (configs/collector.yaml)."""

    model_config = ConfigDict(frozen=True)

    db_url_env: str = Field(default="MARKET_DATA_DATABASE_URL")
    underlyings: list[str] = Field(min_length=1)
    strikes_each_side: int = Field(default=10, gt=0)
    tick_mode: Literal["FULL", "QUOTE", "LTP"] = "FULL"
    recompute_interval_seconds: float = Field(default=30.0, gt=0)
    writer: WriterConfig = Field(default_factory=WriterConfig)
    market_hours: MarketHoursConfig = Field(default_factory=MarketHoursConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    timescale: TimescaleConfig = Field(default_factory=TimescaleConfig)

    @field_validator("underlyings")
    @classmethod
    def _upper_unique(cls, v: list[str]) -> list[str]:
        seen: list[str] = []
        for name in v:
            u = name.upper()
            if u not in seen:
                seen.append(u)
        return seen

    @property
    def instruments_per_underlying(self) -> int:
        """(2N+1) strikes x 2 option types."""
        return (2 * self.strikes_each_side + 1) * 2


def load_collector_config(path: str | Path = "configs/collector.yaml") -> CollectorConfig:
    """Load and validate configs/collector.yaml."""
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
    return CollectorConfig.model_validate(raw)
