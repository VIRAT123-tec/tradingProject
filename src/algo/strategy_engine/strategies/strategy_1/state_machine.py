"""Explicit, persisted state machine for one instrument's daily straddle cycle.

This module owns exactly one thing: *is a proposed Position state change legal,
and record it if so*. It contains no trading logic -- it never decides when to
enter or exit, never places an order, never computes premium/target/SL, and
never talks to a broker. Callers (entry_logic.py, exit_logic.py, the kill
switch, crash recovery) decide *what* transition to make; this module decides
*whether* it is allowed and persists it atomically.

Separation of concerns with the persistence layer
--------------------------------------------------
``PositionRepository.transition_state`` already guarantees the state column and
its audit row are written together in one transaction, but deliberately does
*not* validate legality. This module is that missing validation gate: it checks
the transition against the legal graph, handles idempotent self-transitions,
and only then delegates the write. Every automated state change in the platform
is meant to go through here, so the legal graph lives in exactly one place.

The legal graph
---------------
    IDLE          -> ENTRY_PENDING | ERROR
    ENTRY_PENDING -> OPEN | CLOSED | ERROR
    OPEN          -> EXIT_PENDING | ERROR
    EXIT_PENDING  -> CLOSED | ERROR
    ERROR         -> CLOSED            (resolution; MANUAL/RECOVERY actors only)
    CLOSED        -> (terminal)

``ERROR`` is reachable from every non-terminal state, per the spec's "ERROR
reachable from any state on unrecoverable failure." It is *semi-terminal*: no
automated logic transitions out of it (the owning instance is frozen for manual
review), but an operator or the recovery process may resolve it to ``CLOSED``
once the real position has been squared off -- that single edge is restricted to
the ``MANUAL`` and ``RECOVERY`` actors so a stray strategy tick can never quietly
un-freeze an errored day. ``CLOSED`` is fully terminal: a same-day re-entry is
prevented here (no edge out of CLOSED) as well as by the positions table's
``UNIQUE(strategy_instance_id, trade_date)`` constraint.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from algo.common.enums import PositionState, StateTransitionActor

if TYPE_CHECKING:
    from algo.database.models.position import Position
    from algo.database.models.position_state_transition import PositionStateTransition
    from algo.database.repositories.position_repository import PositionRepository


class IllegalStateTransitionError(Exception):
    """Raised when a proposed transition is not permitted by the legal graph
    (either the states are not adjacent, or the actor is not allowed to make an
    otherwise-adjacent transition).

    Carries the structured ``from_state``/``to_state``/``actor`` so a caller can
    log or branch on them without parsing the message string.
    """

    def __init__(
        self,
        from_state: PositionState,
        to_state: PositionState,
        actor: StateTransitionActor,
        detail: str | None = None,
    ) -> None:
        self.from_state = from_state
        self.to_state = to_state
        self.actor = actor
        message = (
            f"illegal Position state transition {from_state.value} -> "
            f"{to_state.value} (actor={actor.value})"
        )
        if detail:
            message = f"{message}: {detail}"
        super().__init__(message)


# --- The legal graph, as data (pure, importable, unit-testable with no DB) ---

_ALLOWED_TRANSITIONS: dict[PositionState, frozenset[PositionState]] = {
    PositionState.IDLE: frozenset({PositionState.ENTRY_PENDING, PositionState.ERROR}),
    PositionState.ENTRY_PENDING: frozenset(
        {PositionState.OPEN, PositionState.CLOSED, PositionState.ERROR}
    ),
    PositionState.OPEN: frozenset({PositionState.EXIT_PENDING, PositionState.ERROR}),
    PositionState.EXIT_PENDING: frozenset({PositionState.CLOSED, PositionState.ERROR}),
    PositionState.ERROR: frozenset({PositionState.CLOSED}),
    PositionState.CLOSED: frozenset(),
}

# Edges whose legality further depends on *who* is making the transition. Only
# the ERROR -> CLOSED resolution edge is restricted: it must not happen as an
# automated consequence of strategy or risk logic, only a deliberate operator
# action (MANUAL) or the recovery process repairing state against broker truth.
_ACTOR_RESTRICTED_EDGES: dict[
    tuple[PositionState, PositionState], frozenset[StateTransitionActor]
] = {
    (PositionState.ERROR, PositionState.CLOSED): frozenset(
        {StateTransitionActor.MANUAL, StateTransitionActor.RECOVERY}
    ),
}

TERMINAL_STATES: frozenset[PositionState] = frozenset({PositionState.CLOSED})
"""States with no outgoing transition at all -- the cycle is fully done."""

IN_FLIGHT_STATES: frozenset[PositionState] = frozenset(
    {PositionState.ENTRY_PENDING, PositionState.EXIT_PENDING}
)
"""States in which a broker order operation was initiated but its outcome may be
in doubt after a crash. Crash recovery must reconcile these against broker truth
before advancing them (see the module and PositionStateMachine.transition docs).
"""

NON_TERMINAL_STATES: frozenset[PositionState] = frozenset(
    {
        PositionState.IDLE,
        PositionState.ENTRY_PENDING,
        PositionState.OPEN,
        PositionState.EXIT_PENDING,
        PositionState.ERROR,
    }
)
"""Every state except CLOSED -- the states from which ERROR is reachable, and
(matching PositionRepository.list_non_terminal, which also includes ERROR as
still-needing-attention) the set a recovery/monitoring scan considers unfinished.
"""


def allowed_transitions(from_state: PositionState) -> frozenset[PositionState]:
    """Return the set of states reachable in one legal step from ``from_state``
    (ignoring actor restrictions). Empty for a fully terminal state."""
    return _ALLOWED_TRANSITIONS[from_state]


def is_transition_allowed(from_state: PositionState, to_state: PositionState) -> bool:
    """Whether ``from_state -> to_state`` is a legal edge in the graph,
    considering states only (not the actor). Pure; the primary unit-test seam."""
    return to_state in _ALLOWED_TRANSITIONS[from_state]


def is_transition_allowed_for_actor(
    from_state: PositionState, to_state: PositionState, actor: StateTransitionActor
) -> bool:
    """Whether ``from_state -> to_state`` is legal *and* permitted for ``actor``.

    Equivalent to ``is_transition_allowed`` for every edge except the
    actor-restricted ERROR -> CLOSED resolution edge.
    """
    if not is_transition_allowed(from_state, to_state):
        return False
    allowed_actors = _ACTOR_RESTRICTED_EDGES.get((from_state, to_state))
    return allowed_actors is None or actor in allowed_actors


def assert_transition_allowed(
    from_state: PositionState, to_state: PositionState, actor: StateTransitionActor
) -> None:
    """Raise ``IllegalStateTransitionError`` if the transition is not permitted
    for this actor; return ``None`` if it is. Pure -- no persistence, so it is
    trivially unit-testable and also reusable as a pre-check by callers."""
    if not is_transition_allowed(from_state, to_state):
        raise IllegalStateTransitionError(from_state, to_state, actor)
    allowed_actors = _ACTOR_RESTRICTED_EDGES.get((from_state, to_state))
    if allowed_actors is not None and actor not in allowed_actors:
        permitted = ", ".join(sorted(a.value for a in allowed_actors))
        raise IllegalStateTransitionError(
            from_state,
            to_state,
            actor,
            detail=f"this edge is restricted to actors {{{permitted}}}",
        )


def is_terminal(state: PositionState) -> bool:
    """Whether ``state`` has no outgoing transitions (i.e. is CLOSED)."""
    return state in TERMINAL_STATES


def is_in_flight(state: PositionState) -> bool:
    """Whether ``state`` is one whose in-doubt broker operation crash recovery
    must reconcile (ENTRY_PENDING or EXIT_PENDING)."""
    return state in IN_FLIGHT_STATES


class PositionStateMachine:
    """Validates and records Position state transitions against the legal graph.

    Constructed with a ``PositionRepository`` (which carries the unit-of-work's
    ``Session``), so it is created per unit of work by the caller that already
    holds one. It is otherwise stateless: all rules live in the module-level
    pure functions above, which is what keeps this both unit-test-friendly (test
    the rules with no database, test ``transition`` with a fake repository) and
    the single source of truth for transition legality.
    """

    def __init__(self, repository: PositionRepository) -> None:
        self._repository = repository

    def transition(
        self,
        position: Position,
        *,
        to_state: PositionState,
        actor: StateTransitionActor,
        reason: str | None = None,
    ) -> PositionStateTransition | None:
        """Move ``position`` to ``to_state``, recording the audit row, if the
        transition is legal for ``actor``.

        Idempotency: a request to transition to the state the position is
        already in is a no-op -- it returns ``None`` and writes nothing (no
        state change, no audit row). This is what lets crash/restart recovery
        re-apply the same computed target state repeatedly without erroring or
        spamming the audit log. Broader whole-cycle idempotency (not entering
        twice) is enforced separately by the positions table's unique
        constraint and the order_intents design, not here.

        Concurrency: the underlying write goes through the repository, which
        bumps the Position's optimistic-lock ``version``. If another writer
        (e.g. the independent kill switch racing the strategy's own exit)
        modified the row since it was loaded, the flush raises
        ``ConcurrentModificationError``; this method lets it propagate -- whether
        to reload-and-retry is the caller's business decision, not the state
        machine's.

        Raises ``IllegalStateTransitionError`` if the transition is not
        permitted; in that case nothing is written. ``position`` must already be
        persisted (have an id) so the audit row's foreign key resolves.

        Returns the created ``PositionStateTransition`` on a real transition, or
        ``None`` for an idempotent no-op.
        """
        from_state = position.state
        if from_state == to_state:
            return None
        assert_transition_allowed(from_state, to_state, actor)
        return self._repository.transition_state(
            position, to_state=to_state, actor=actor, reason=reason
        )

    def mark_error(
        self,
        position: Position,
        *,
        actor: StateTransitionActor,
        reason: str,
    ) -> PositionStateTransition | None:
        """Convenience for the safety-critical move to ERROR, readable at the
        call sites that matter (error handlers, the kill switch).

        Reachable from every non-terminal state, so it succeeds from IDLE,
        ENTRY_PENDING, OPEN, or EXIT_PENDING. A position already in ERROR is an
        idempotent no-op (returns ``None``); a CLOSED position raises
        ``IllegalStateTransitionError`` (a completed cycle cannot be re-opened
        into an error -- a post-close discrepancy is a reconciliation concern,
        handled via the reconciliation_breaks table and the
        ``needs_reconciliation`` flag, not by un-terminating the state).

        ``reason`` is required here (unlike the general ``transition``) because
        an ERROR transition without a recorded cause is close to useless during
        a post-incident review.
        """
        return self.transition(
            position, to_state=PositionState.ERROR, actor=actor, reason=reason
        )

    @staticmethod
    def current_state(position: Position) -> PositionState:
        """Return the authoritative current state -- read straight off the
        persisted Position row.

        This is the whole of "restart recovery" at the state-machine level: the
        database is the source of truth, so a restarted process reconstructs
        where a position is simply by reading this, never by assuming IDLE. What
        to *do* from that state (resume monitoring an OPEN position, reconcile an
        in-flight one) is the strategy's and recovery's job, not this module's.
        """
        return position.state
