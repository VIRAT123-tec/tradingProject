"""Kite (Zerodha) broker implementation of broker_base.py's interface. Built only
after the simulation broker and strategy logic are working end-to-end, per the
delivery plan.

All Kite-specific knowledge is isolated to this package: ``mapper`` (request/
response/exception mapping), ``kite_auth`` (token lifecycle), ``websocket``
(order-update stream), and ``kite_broker`` (the BrokerBase implementation).
"""

from algo.brokers.kite.kite_auth import AccessTokenStore, KiteSession
from algo.brokers.kite.kite_broker import KiteBroker, KiteBrokerConfig, KiteClientProtocol
from algo.brokers.kite.mapper import KiteMappingError
from algo.brokers.kite.token_manager import TokenManager
from algo.brokers.kite.token_store import EnvFileTokenStore, TokenRecord, TokenStore
from algo.brokers.kite.websocket import (
    KiteOrderUpdateStream,
    KiteTickerFactory,
    KiteTickerProtocol,
)

__all__ = [
    "KiteBroker",
    "KiteBrokerConfig",
    "KiteClientProtocol",
    "KiteSession",
    "AccessTokenStore",
    "KiteOrderUpdateStream",
    "KiteTickerProtocol",
    "KiteTickerFactory",
    "KiteMappingError",
    "TokenManager",
    "TokenStore",
    "TokenRecord",
    "EnvFileTokenStore",
]
