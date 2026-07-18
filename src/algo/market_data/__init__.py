"""Market data ingestion: websocket tick handling, candle building, option chain
construction, and the shared in-memory market cache.

The cache in this package is derived/ephemeral and rebuildable from live ticks after
a restart — it is never the source of truth for position or strategy state (the
database is)."""

from algo.market_data.market_cache import CachedPrice, MarketCache
from algo.market_data.market_data_service import (
    MarketDataConfig,
    MarketDataService,
    MarketDataUnavailableError,
)
from algo.market_data.subscription_manager import SubscriptionManager
from algo.market_data.tick_router import TickRouter, is_plausible
from algo.market_data.websocket_manager import ConnectionState, LtpPoller, TickStream

__all__ = [
    "MarketCache",
    "CachedPrice",
    "SubscriptionManager",
    "TickRouter",
    "is_plausible",
    "TickStream",
    "LtpPoller",
    "ConnectionState",
    "MarketDataService",
    "MarketDataConfig",
    "MarketDataUnavailableError",
]
