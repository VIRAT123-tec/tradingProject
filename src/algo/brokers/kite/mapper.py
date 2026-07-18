"""All Kite-specific mapping, isolated in one module.

Every conversion between the platform's broker-agnostic vocabulary (the enums
in ``algo.common.enums`` and the DTOs in ``algo.brokers.broker_base``) and
Kite Connect's own string constants / response dict shapes lives here, and
nowhere else. ``kite_broker.py`` and ``websocket.py`` call these pure functions;
they never touch a Kite string constant directly. This is what keeps "Kite
knowledge" out of the rest of the platform -- swapping in a different broker
means writing a new mapper, not editing the strategy or execution code.

Also here: ``translate_kite_exception`` -- mapping the exceptions kiteconnect
raises (and the raw ``requests`` exceptions it re-raises on transport failures)
onto this platform's ``BrokerError`` hierarchy. Getting this mapping right is
safety-critical: it is what decides, for a failed order placement, whether the
outcome is treated as a definitive rejection, a never-happened connection
failure, or an *ambiguous* timeout that must not be retried.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import pytz
import requests.exceptions as req_exc
from kiteconnect import KiteConnect
from kiteconnect import exceptions as kite_exc

from algo.brokers.broker_base import (
    BrokerHolding,
    BrokerInstrument,
    BrokerMargins,
    BrokerOrder,
    BrokerPosition,
    BrokerQuote,
    InstrumentIdentifier,
    PlaceOrderRequest,
)
from algo.brokers.exceptions import (
    BrokerAuthenticationError,
    BrokerConnectionError,
    BrokerError,
    BrokerTimeoutError,
    InstrumentNotFoundError,
    InvalidOrderRequestError,
    OrderRejectedError,
)
from algo.common.enums import Exchange, OptionType, OrderStatus, OrderType, ProductType, TransactionType

_IST = pytz.timezone("Asia/Kolkata")


class KiteMappingError(BrokerError):
    """A Kite response could not be mapped onto a platform DTO (an unexpected
    shape or an unmappable value). Distinct from a broker *communication*
    failure -- this means Kite answered, but with something we did not expect."""


# ---------------------------------------------------------------------------
# Enum mapping (platform -> Kite)
# ---------------------------------------------------------------------------

_EXCHANGE_TO_KITE: dict[Exchange, str] = {
    Exchange.NFO: KiteConnect.EXCHANGE_NFO,
    Exchange.BFO: KiteConnect.EXCHANGE_BFO,
    # Cash segments -- never used for a persisted order/position (those are
    # always NFO/BFO), only for reading an underlying index's spot LTP
    # (SpotPriceProvider). See Exchange's own docstring in common/enums.py.
    Exchange.NSE: KiteConnect.EXCHANGE_NSE,
    Exchange.BSE: KiteConnect.EXCHANGE_BSE,
}
_TRANSACTION_TO_KITE: dict[TransactionType, str] = {
    TransactionType.BUY: KiteConnect.TRANSACTION_TYPE_BUY,
    TransactionType.SELL: KiteConnect.TRANSACTION_TYPE_SELL,
}
_PRODUCT_TO_KITE: dict[ProductType, str] = {
    ProductType.INTRADAY: KiteConnect.PRODUCT_MIS,
    ProductType.NORMAL: KiteConnect.PRODUCT_NRML,
}
_ORDER_TYPE_TO_KITE: dict[OrderType, str] = {
    OrderType.MARKET: KiteConnect.ORDER_TYPE_MARKET,
    OrderType.LIMIT: KiteConnect.ORDER_TYPE_LIMIT,
}


def to_kite_exchange(exchange: Exchange) -> str:
    return _EXCHANGE_TO_KITE[exchange]


def to_kite_transaction_type(transaction_type: TransactionType) -> str:
    return _TRANSACTION_TO_KITE[transaction_type]


def to_kite_product(product: ProductType) -> str:
    return _PRODUCT_TO_KITE[product]


def to_kite_order_type(order_type: OrderType) -> str:
    return _ORDER_TYPE_TO_KITE[order_type]


# ---------------------------------------------------------------------------
# Enum mapping (Kite -> platform)
# ---------------------------------------------------------------------------

_EXCHANGE_FROM_KITE = {v: k for k, v in _EXCHANGE_TO_KITE.items()}
_TRANSACTION_FROM_KITE = {v: k for k, v in _TRANSACTION_TO_KITE.items()}
_PRODUCT_FROM_KITE = {v: k for k, v in _PRODUCT_TO_KITE.items()}

# Kite exposes many transient order statuses; every non-terminal one collapses
# to PENDING (still in flight) so callers keep polling, and only the three true
# terminal statuses map to their terminal counterparts. An unrecognised status
# maps to PENDING (never a terminal state) so an order is never *wrongly*
# treated as finished -- the safe direction to err.
_STATUS_FROM_KITE: dict[str, OrderStatus] = {
    "COMPLETE": OrderStatus.COMPLETE,
    "REJECTED": OrderStatus.REJECTED,
    "CANCELLED": OrderStatus.CANCELLED,
    "OPEN": OrderStatus.OPEN,
}


def from_kite_exchange(value: str) -> Exchange:
    try:
        return _EXCHANGE_FROM_KITE[value]
    except KeyError:
        raise KiteMappingError(f"unmappable Kite exchange {value!r}") from None


def from_kite_status(value: str) -> OrderStatus:
    return _STATUS_FROM_KITE.get((value or "").upper(), OrderStatus.PENDING)


def from_kite_product(value: str) -> ProductType:
    # Foreign orders (CNC/CO placed manually) are not something this platform
    # trades; default them to NORMAL rather than crash a whole-orderbook parse.
    return _PRODUCT_FROM_KITE.get(value, ProductType.NORMAL)


def from_kite_order_type(value: str) -> OrderType:
    # Only MARKET/LIMIT are in the platform vocabulary; SL/SL-M (which this
    # platform never places) best-effort map to LIMIT.
    return OrderType.MARKET if value == KiteConnect.ORDER_TYPE_MARKET else OrderType.LIMIT


def from_kite_option_type(value: str) -> OptionType | None:
    if value == "CE":
        return OptionType.CE
    if value == "PE":
        return OptionType.PE
    return None


# ---------------------------------------------------------------------------
# Value helpers
# ---------------------------------------------------------------------------


def _decimal(value: Any) -> Decimal | None:
    """Convert a Kite numeric (float/int/str) to Decimal via str() to avoid
    float artefacts. Returns None for None/empty; raises KiteMappingError for a
    genuinely unparseable value."""
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise KiteMappingError(f"cannot convert {value!r} to Decimal") from exc


def _decimal_or_zero(value: Any) -> Decimal:
    result = _decimal(value)
    return result if result is not None else Decimal("0")


def _price_or_none(value: Any) -> Decimal | None:
    """A price field: Kite reports 0 for "no price yet" (e.g. average_price
    before any fill); normalise that to None."""
    result = _decimal(value)
    return result if result and result != 0 else None


def _localize(value: Any) -> datetime | None:
    """Kite returns naive IST datetimes (already parsed by kiteconnect).
    Attach IST tzinfo so downstream comparisons are timezone-correct; pass
    through anything already aware, and None for missing."""
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return _IST.localize(value)
    return value


# ---------------------------------------------------------------------------
# Requests (platform -> Kite kwargs)
# ---------------------------------------------------------------------------


def place_order_kwargs(request: PlaceOrderRequest) -> dict[str, Any]:
    """Build the keyword arguments for ``KiteConnect.place_order`` from a
    platform ``PlaceOrderRequest``. Always variety=regular (this platform
    places only regular orders). ``price``/``trigger_price`` are passed as
    plain floats because that is what kiteconnect expects on the wire."""
    kwargs: dict[str, Any] = {
        "variety": KiteConnect.VARIETY_REGULAR,
        "exchange": to_kite_exchange(request.exchange),
        "tradingsymbol": request.tradingsymbol,
        "transaction_type": to_kite_transaction_type(request.transaction_type),
        "quantity": request.quantity,
        "product": to_kite_product(request.product),
        "order_type": to_kite_order_type(request.order_type),
        "tag": request.tag,
    }
    if request.price is not None:
        kwargs["price"] = float(request.price)
    if request.trigger_price is not None:
        kwargs["trigger_price"] = float(request.trigger_price)
    return kwargs


def modify_order_kwargs(
    *, order_id: str, quantity: int | None, price: Decimal | None,
    trigger_price: Decimal | None, order_type: OrderType | None,
) -> dict[str, Any]:
    """Build kwargs for ``KiteConnect.modify_order`` -- only the fields the
    caller actually wants changed are included."""
    kwargs: dict[str, Any] = {"variety": KiteConnect.VARIETY_REGULAR, "order_id": order_id}
    if quantity is not None:
        kwargs["quantity"] = quantity
    if price is not None:
        kwargs["price"] = float(price)
    if trigger_price is not None:
        kwargs["trigger_price"] = float(trigger_price)
    if order_type is not None:
        kwargs["order_type"] = to_kite_order_type(order_type)
    return kwargs


def quote_key(instrument: InstrumentIdentifier) -> str:
    """Kite's quote/ltp instrument key format: 'EXCHANGE:TRADINGSYMBOL'."""
    return f"{to_kite_exchange(instrument.exchange)}:{instrument.tradingsymbol}"


# ---------------------------------------------------------------------------
# Responses (Kite dict -> platform DTO)
# ---------------------------------------------------------------------------


def to_broker_order(data: dict[str, Any]) -> BrokerOrder:
    """Map one Kite order dict (from ``orders()``, ``order_history()``, or a
    websocket order postback) onto a ``BrokerOrder``."""
    try:
        filled = int(data.get("filled_quantity") or 0)
        quantity = int(data.get("quantity") or 0)
        return BrokerOrder(
            broker_order_id=str(data["order_id"]),
            tag=(data.get("tag") or None),
            status=from_kite_status(data.get("status", "")),
            exchange=from_kite_exchange(data["exchange"]),
            tradingsymbol=data["tradingsymbol"],
            transaction_type=_TRANSACTION_FROM_KITE.get(
                data.get("transaction_type", ""), TransactionType.BUY
            ),
            product=from_kite_product(data.get("product", "")),
            order_type=from_kite_order_type(data.get("order_type", "")),
            quantity=quantity,
            filled_quantity=filled,
            average_price=_price_or_none(data.get("average_price")),
            trigger_price=_price_or_none(data.get("trigger_price")),
            placed_at=_localize(data.get("order_timestamp")),
            filled_at=_localize(data.get("exchange_timestamp")) if filled > 0 else None,
            exchange_updated_at=_localize(
                data.get("exchange_update_timestamp") or data.get("exchange_timestamp")
            ),
            status_message=(data.get("status_message") or None),
            raw_response=dict(data),
        )
    except KiteMappingError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise KiteMappingError(f"cannot map Kite order dict: {exc}") from exc


def to_broker_position(data: dict[str, Any]) -> BrokerPosition:
    try:
        return BrokerPosition(
            exchange=from_kite_exchange(data["exchange"]),
            tradingsymbol=data["tradingsymbol"],
            product=from_kite_product(data.get("product", "")),
            quantity=int(data.get("quantity") or 0),
            average_price=_decimal_or_zero(data.get("average_price")),
            last_price=_decimal_or_zero(data.get("last_price")),
            pnl=_decimal_or_zero(data.get("pnl")),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise KiteMappingError(f"cannot map Kite position dict: {exc}") from exc


def to_broker_holding(data: dict[str, Any]) -> BrokerHolding:
    try:
        return BrokerHolding(
            exchange=from_kite_exchange(data["exchange"]),
            tradingsymbol=data["tradingsymbol"],
            quantity=int(data.get("quantity") or 0),
            average_price=_decimal_or_zero(data.get("average_price")),
            last_price=_decimal_or_zero(data.get("last_price")),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise KiteMappingError(f"cannot map Kite holding dict: {exc}") from exc


def to_broker_margins(segment: str, data: dict[str, Any]) -> BrokerMargins:
    """Map one Kite margin *segment* block (e.g. the 'equity' block of
    ``margins()``) onto ``BrokerMargins``."""
    try:
        available = data.get("available", {}) or {}
        utilised = data.get("utilised", {}) or {}
        used = utilised.get("debits")
        if used is None:
            used = data.get("net")
        return BrokerMargins(
            segment=segment,
            available_cash=_decimal_or_zero(available.get("cash") or available.get("live_balance")),
            used_margin=_decimal_or_zero(used),
            opening_balance=_decimal_or_zero(available.get("opening_balance")),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise KiteMappingError(f"cannot map Kite margins dict: {exc}") from exc


def to_broker_quote(instrument: InstrumentIdentifier, data: dict[str, Any]) -> BrokerQuote:
    try:
        ohlc = data.get("ohlc", {}) or {}
        return BrokerQuote(
            instrument=instrument,
            last_price=_decimal_or_zero(data.get("last_price")),
            ohlc_open=_decimal_or_zero(ohlc.get("open")),
            ohlc_high=_decimal_or_zero(ohlc.get("high")),
            ohlc_low=_decimal_or_zero(ohlc.get("low")),
            ohlc_close=_decimal_or_zero(ohlc.get("close")),
            volume=int(data.get("volume") or data.get("volume_traded") or 0),
            timestamp=_localize(data.get("timestamp")) or datetime.now(_IST),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise KiteMappingError(f"cannot map Kite quote dict: {exc}") from exc


def to_broker_instrument(data: dict[str, Any]) -> BrokerInstrument:
    """Map one row of Kite's ``instruments()`` dump onto a ``BrokerInstrument``."""
    try:
        expiry = data.get("expiry")
        expiry_date = expiry if isinstance(expiry, date) else None
        strike = _price_or_none(data.get("strike"))
        return BrokerInstrument(
            instrument_token=int(data["instrument_token"]),
            exchange=from_kite_exchange(data["exchange"]),
            tradingsymbol=data["tradingsymbol"],
            name=data.get("name") or data["tradingsymbol"],
            lot_size=int(data.get("lot_size") or 1),
            tick_size=_decimal_or_zero(data.get("tick_size")) or Decimal("0.05"),
            expiry=expiry_date,
            strike=strike,
            option_type=from_kite_option_type(data.get("instrument_type", "")),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise KiteMappingError(f"cannot map Kite instrument dict: {exc}") from exc


# ---------------------------------------------------------------------------
# Exception mapping (kiteconnect / requests -> BrokerError)
# ---------------------------------------------------------------------------


def translate_kite_exception(exc: BaseException, *, mutating: bool) -> BrokerError:
    """Translate an exception raised by a Kite client call into this platform's
    ``BrokerError`` hierarchy.

    The ``mutating`` flag is the safety-critical parameter. For a mutating call
    (place/modify/cancel), a transport error whose "did it reach the exchange?"
    answer is unknown -- a read timeout, or a Kite-reported network/OMS error --
    must surface as ``BrokerTimeoutError`` (the *ambiguous* signal callers must
    never blindly retry). For a read, the same underlying error is merely a
    retryable connection blip. Errors that are unambiguously "never sent" (a
    connect timeout, a connection error before the request left) are
    ``BrokerConnectionError`` in both cases.
    """
    # Definitive, Kite-answered errors (an HTTP response came back).
    if isinstance(exc, kite_exc.TokenException):
        return BrokerAuthenticationError(f"Kite session invalid/expired: {exc}")
    if isinstance(exc, kite_exc.PermissionException):
        return BrokerAuthenticationError(f"Kite permission denied: {exc}")
    if isinstance(exc, kite_exc.InputException):
        return InvalidOrderRequestError(f"Kite rejected the request as invalid: {exc}")
    if isinstance(exc, kite_exc.OrderException):
        return OrderRejectedError(f"Kite order error: {exc}")

    # Transport-level: kiteconnect re-raises the raw requests exceptions.
    if isinstance(exc, req_exc.ConnectTimeout):
        return BrokerConnectionError(f"Kite connect timeout (request not sent): {exc}")
    if isinstance(exc, req_exc.ReadTimeout):
        return BrokerTimeoutError(f"Kite read timeout (outcome unknown): {exc}")
    if isinstance(exc, req_exc.Timeout):
        return BrokerTimeoutError(f"Kite request timeout (outcome unknown): {exc}")
    if isinstance(exc, req_exc.ConnectionError):
        return BrokerConnectionError(f"Kite connection error: {exc}")

    # Kite-reported network/OMS trouble: ambiguous for a mutation, retryable
    # for a read.
    if isinstance(exc, kite_exc.NetworkException):
        if mutating:
            return BrokerTimeoutError(f"Kite network/OMS error, outcome unknown: {exc}")
        return BrokerConnectionError(f"Kite network/OMS error: {exc}")

    if isinstance(exc, kite_exc.DataException):
        return KiteMappingError(f"Kite returned malformed data: {exc}")
    if isinstance(exc, kite_exc.GeneralException):
        return BrokerError(f"Kite general error: {exc}")

    # Anything else (including our own already-translated BrokerError, which a
    # caller may pass through): wrap unknowns, pass BrokerErrors through.
    if isinstance(exc, BrokerError):
        return exc
    return BrokerError(f"unexpected error from Kite client: {exc!r}")


def is_order_not_found(exc: BaseException) -> bool:
    """Whether a Kite exception indicates the order id is unknown to the broker
    (used to raise OrderNotFoundError from get_order). Kite reports this as an
    InputException/GeneralException whose message mentions the order id."""
    if isinstance(exc, (kite_exc.InputException, kite_exc.GeneralException)):
        message = str(exc).lower()
        return "order" in message and ("not found" in message or "invalid" in message)
    return False


def is_instrument_not_found(exc: BaseException) -> bool:
    """Whether a Kite exception indicates an unknown instrument (used to raise
    InstrumentNotFoundError from quote/ltp/get_instrument)."""
    return isinstance(exc, kite_exc.InputException)
