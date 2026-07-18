"""Broker-layer exceptions.

Concrete brokers (kite/, simulation/) translate their own internal errors
(HTTP failures, kiteconnect's exceptions, etc.) into these types, so callers
-- execution/, risk/, strategy_engine/ -- depend only on this module's
vocabulary, never on a specific broker SDK's exception classes.

The Retryable/NonRetryable split is the one every other design choice in
brokers/ hangs off: a generic retry decorator (common/decorators.py, still a
TODO) can safely wrap any read-only BrokerBase method on RetryableBrokerError
without knowing anything about brokers. It must NOT be used to blindly wrap
place_order, modify_order, or cancel_order -- see BrokerTimeoutError below.
"""

from __future__ import annotations


class BrokerError(Exception):
    """Base class for all broker-layer exceptions."""


class RetryableBrokerError(BrokerError):
    """The call did not reach/complete at the broker, or reaching it again is
    known to be safe -- a generic retry-with-backoff wrapper may act on this
    class alone for read-only methods."""


class NonRetryableBrokerError(BrokerError):
    """Retrying will not help: the broker understood the request and gave a
    definitive answer (rejection, not-found, already-terminal), or the
    session itself needs human/operator intervention."""


class BrokerConnectionError(RetryableBrokerError):
    """Network/transport failure before any response was received."""


class BrokerTimeoutError(RetryableBrokerError):
    """No response arrived within the caller's timeout budget.

    When raised by a read-only method, safe to retry. When raised by
    place_order, modify_order, or cancel_order specifically, the outcome is
    UNKNOWN -- the broker may have processed the request before the timeout
    fired. Never blindly retry those three methods on this exception; resolve
    the ambiguity first via find_order_by_tag / get_order (this is exactly
    the SUBMITTED_UNCONFIRMED window the order_intents table exists for).
    """


class BrokerRateLimitExceededError(RetryableBrokerError):
    """The broker's (or this platform's own self-throttling) rate limit was
    hit and no slot will free within the caller's timeout budget.

    Unlike BrokerTimeoutError on a mutating call, this is unambiguous: the
    request was never sent, so retrying after backoff is always safe.
    """


class BrokerAuthenticationError(NonRetryableBrokerError):
    """The session is invalid or expired. Requires re-authentication (e.g. a
    fresh Kite access token via kite_auth.py), not a retry of the same call.
    """


class OrderRejectedError(NonRetryableBrokerError):
    """The broker rejected the order outright (bad symbol, insufficient
    margin, market closed, etc.)."""


class OrderNotFoundError(NonRetryableBrokerError):
    """The broker has no record of the given broker_order_id."""


class OrderNotModifiableError(NonRetryableBrokerError):
    """The order is already in a terminal state and cannot be modified."""


class OrderNotCancellableError(NonRetryableBrokerError):
    """The order is already in a terminal state and cannot be cancelled."""


class InstrumentNotFoundError(NonRetryableBrokerError):
    """No instrument matches the given lookup criteria."""


class InvalidOrderRequestError(NonRetryableBrokerError):
    """The broker rejected the request as structurally invalid, independent
    of market conditions (e.g. a field combination the broker's API itself
    disallows). Distinct from OrderRejectedError, which is a trading-rules
    rejection (margin, symbol) rather than a malformed-request rejection.
    """
