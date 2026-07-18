"""Shared enum types used across the database models and, later, the strategy engine.

Every enum here is stored in the database as a validated string (SQLAlchemy
``Enum(..., native_enum=False)``), not a native Postgres ENUM type. This is a
deliberate portability/maintainability choice: adding a new member later is a
metadata-only change enforced by a CHECK constraint, and never requires an
``ALTER TYPE ... ADD VALUE`` migration, which has awkward transactional
restrictions on Postgres and complicates Alembic autogeneration.

Enum member name and value are always identical (e.g. ``PENDING = "PENDING"``)
so the stored string is unambiguous when read directly from the database.
"""

from enum import Enum


class BrokerName(str, Enum):
    """Which broker backs an account."""

    KITE = "KITE"
    SIMULATION = "SIMULATION"


class Exchange(str, Enum):
    """Exchange segment an instrument trades on.

    This is fixed exchange vocabulary (like OptionType or TransactionType), not
    instrument-specific configuration -- unlike instrument *names* (Nifty,
    Sensex, ...), which are intentionally left as plain config-driven strings
    rather than an enum, since new instruments can be onboarded via config
    without a schema change.

    NFO/BFO are the F&O (derivatives) segments -- the only ones ever written to
    a persisted ``exchange`` column (positions/trades are always options). NSE
    and BSE are the cash segments, used *only* for reading an underlying index's
    spot LTP (SpotPriceProvider); they are never persisted, so adding them does
    not affect the derivatives-only CHECK constraints on the database columns.
    """

    NFO = "NFO"
    BFO = "BFO"
    NSE = "NSE"
    BSE = "BSE"


class OptionType(str, Enum):
    CE = "CE"
    PE = "PE"


class InstanceStatus(str, Enum):
    """Operational status of a persistent StrategyInstance.

    FROZEN is the "freeze that instrument's instance, not the whole platform"
    state reachable from an unrecoverable ERROR on one of its positions. A
    frozen instance's entry scheduler must refuse to arm it until a human
    clears it back to ACTIVE.
    """

    ACTIVE = "ACTIVE"
    FROZEN = "FROZEN"
    DISABLED = "DISABLED"


class PositionState(str, Enum):
    """Explicit, persisted state machine for one instrument's daily straddle cycle."""

    IDLE = "IDLE"
    ENTRY_PENDING = "ENTRY_PENDING"
    OPEN = "OPEN"
    EXIT_PENDING = "EXIT_PENDING"
    CLOSED = "CLOSED"
    ERROR = "ERROR"


class ExitReason(str, Enum):
    TARGET = "TARGET"
    STOPLOSS = "STOPLOSS"
    TIMEOUT = "TIMEOUT"
    KILL_SWITCH = "KILL_SWITCH"
    MANUAL = "MANUAL"
    ERROR = "ERROR"


class TradeLegStatus(str, Enum):
    """Lifecycle of a single option leg (CE or PE), simpler than the parent
    Position state machine.

    UNWOUND is distinct from CLOSED: it means this leg was bought back as part
    of the auto-unwind partial-entry policy (the other leg never filled), not
    as the normal outcome of an OPEN position hitting target/SL/cutoff/kill-switch.
    """

    PENDING = "PENDING"
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    UNWOUND = "UNWOUND"
    ERROR = "ERROR"


class OrderPurpose(str, Enum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"


class TransactionType(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(str, Enum):
    """Only MARKET is used by strategy-1 today; kept as a field (not a hardcoded
    literal) so a future strategy or exit-slippage guard can use LIMIT without
    a schema change."""

    MARKET = "MARKET"
    LIMIT = "LIMIT"


class ProductType(str, Enum):
    """Margin/settlement product for an order, named at a broker-agnostic
    level deliberately -- not Kite's own MIS/NRML/CNC vocabulary. Translating
    to a specific broker's product codes is that broker's mapper's job, not
    something the rest of the platform should need to know.
    """

    INTRADAY = "INTRADAY"
    NORMAL = "NORMAL"


class OrderStatus(str, Enum):
    """Mirrors the broker order lifecycle (Kite's order statuses collapse
    cleanly onto this set)."""

    PENDING = "PENDING"
    OPEN = "OPEN"
    COMPLETE = "COMPLETE"
    REJECTED = "REJECTED"
    CANCELLED = "CANCELLED"
    ERROR = "ERROR"


class IntentStatus(str, Enum):
    """Lifecycle of an order_intent -- the durable "about to place this order"
    record written before any broker call.

    SUBMITTED_UNCONFIRMED is distinct from PLACED: PLACED means we received a
    broker_order_id back; SUBMITTED_UNCONFIRMED means the broker call was made
    but the response never reached us (e.g. a network timeout), so we do not
    yet know whether the order exists at the broker. This distinction is what
    lets crash recovery tell "never sent" apart from "sent, ack lost" -- the
    two require different recovery actions.
    """

    PENDING = "PENDING"
    SUBMITTED_UNCONFIRMED = "SUBMITTED_UNCONFIRMED"
    PLACED = "PLACED"
    CONFIRMED = "CONFIRMED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class StateTransitionActor(str, Enum):
    """Who/what caused a position state transition, recorded on every row of
    position_state_transitions for audit purposes."""

    STRATEGY = "STRATEGY"
    RISK_MANAGER = "RISK_MANAGER"
    KILL_SWITCH = "KILL_SWITCH"
    RECOVERY = "RECOVERY"
    MANUAL = "MANUAL"


class RiskFlagType(str, Enum):
    KILL_SWITCH = "KILL_SWITCH"
    EMERGENCY_EXIT = "EMERGENCY_EXIT"
    FREEZE = "FREEZE"


class RiskFlagScope(str, Enum):
    """Scope a risk_control_flag applies to. GLOBAL affects the whole platform,
    ACCOUNT affects every strategy instance under one account, STRATEGY_INSTANCE
    affects exactly one instrument's instance."""

    GLOBAL = "GLOBAL"
    ACCOUNT = "ACCOUNT"
    STRATEGY_INSTANCE = "STRATEGY_INSTANCE"


class ReconciliationBreakType(str, Enum):
    """Classifies a detected discrepancy between broker truth and DB state."""

    ORPHAN_FILL = "ORPHAN_FILL"
    BROKER_FLAT_DB_OPEN = "BROKER_FLAT_DB_OPEN"
    AMBIGUOUS_ACK = "AMBIGUOUS_ACK"
    PARTIAL_ENTRY = "PARTIAL_ENTRY"
    QUANTITY_MISMATCH = "QUANTITY_MISMATCH"
    OTHER = "OTHER"


class ReconciliationResolution(str, Enum):
    PENDING = "PENDING"
    AUTO_REPAIRED = "AUTO_REPAIRED"
    MANUAL = "MANUAL"
