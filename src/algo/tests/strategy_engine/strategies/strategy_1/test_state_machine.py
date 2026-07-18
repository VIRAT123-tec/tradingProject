"""Unit tests for the Strategy-1 Position state machine.

Flagged in the spec as a piece most likely to have subtle, money-costing bugs,
so it is tested exhaustively and in isolation. The pure legality rules need no
database; ``PositionStateMachine.transition`` is tested against a fake
repository that mimics ``PositionRepository.transition_state`` without any real
persistence.
"""

from __future__ import annotations

import itertools

import pytest

from algo.common.enums import PositionState, StateTransitionActor
from algo.strategy_engine.strategies.strategy_1.state_machine import (
    IN_FLIGHT_STATES,
    NON_TERMINAL_STATES,
    TERMINAL_STATES,
    IllegalStateTransitionError,
    PositionStateMachine,
    allowed_transitions,
    assert_transition_allowed,
    is_in_flight,
    is_terminal,
    is_transition_allowed,
    is_transition_allowed_for_actor,
)

S = PositionState
A = StateTransitionActor

# The authoritative legal graph, restated independently here so the test fails
# if the module's table is edited without a matching, deliberate test change.
_LEGAL_EDGES: dict[PositionState, set[PositionState]] = {
    S.IDLE: {S.ENTRY_PENDING, S.ERROR},
    S.ENTRY_PENDING: {S.OPEN, S.CLOSED, S.ERROR},
    S.OPEN: {S.EXIT_PENDING, S.ERROR},
    S.EXIT_PENDING: {S.CLOSED, S.ERROR},
    S.ERROR: {S.CLOSED},
    S.CLOSED: set(),
}

_ALL_ACTORS = list(StateTransitionActor)


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------


class FakePosition:
    """Minimal stand-in for the Position ORM row: just an id and a mutable
    state, which is all the state machine touches."""

    def __init__(self, state: PositionState, id_: int = 1) -> None:
        self.id = id_
        self.state = state


class FakeTransition:
    """Sentinel returned by the fake repository to stand in for a persisted
    PositionStateTransition row."""

    def __init__(self, from_state, to_state, actor, reason) -> None:
        self.from_state = from_state
        self.to_state = to_state
        self.actor = actor
        self.reason = reason


class FakeRepository:
    """Mimics PositionRepository.transition_state: mutates the position's state
    and records the call, without any database."""

    def __init__(self) -> None:
        self.calls: list[FakeTransition] = []

    def transition_state(self, position, *, to_state, actor, reason=None):
        transition = FakeTransition(position.state, to_state, actor, reason)
        position.state = to_state
        self.calls.append(transition)
        return transition


# --------------------------------------------------------------------------
# Pure legality rules
# --------------------------------------------------------------------------


class TestLegalGraph:
    def test_allowed_transitions_matches_specification(self):
        for from_state, expected in _LEGAL_EDGES.items():
            assert set(allowed_transitions(from_state)) == expected

    def test_every_state_pair_matches_the_table(self):
        # Exhaustive over all 6x6 ordered pairs: the single source of truth is
        # _LEGAL_EDGES, and is_transition_allowed must agree on every one.
        for from_state, to_state in itertools.product(S, S):
            expected = to_state in _LEGAL_EDGES[from_state]
            assert is_transition_allowed(from_state, to_state) is expected, (
                from_state,
                to_state,
            )

    def test_error_reachable_from_every_non_terminal_state(self):
        for state in (S.IDLE, S.ENTRY_PENDING, S.OPEN, S.EXIT_PENDING):
            assert is_transition_allowed(state, S.ERROR)

    def test_closed_is_fully_terminal(self):
        assert allowed_transitions(S.CLOSED) == frozenset()
        assert is_terminal(S.CLOSED)
        for to_state in S:
            if to_state is not S.CLOSED:
                assert not is_transition_allowed(S.CLOSED, to_state)

    def test_error_is_semi_terminal_only_to_closed(self):
        assert allowed_transitions(S.ERROR) == frozenset({S.CLOSED})
        assert not is_terminal(S.ERROR)

    def test_classification_sets(self):
        assert TERMINAL_STATES == frozenset({S.CLOSED})
        assert IN_FLIGHT_STATES == frozenset({S.ENTRY_PENDING, S.EXIT_PENDING})
        assert NON_TERMINAL_STATES == frozenset(S) - {S.CLOSED}
        assert is_in_flight(S.ENTRY_PENDING) and is_in_flight(S.EXIT_PENDING)
        assert not is_in_flight(S.OPEN)


class TestActorRestrictions:
    @pytest.mark.parametrize("actor", [A.MANUAL, A.RECOVERY])
    def test_error_to_closed_allowed_for_manual_and_recovery(self, actor):
        assert is_transition_allowed_for_actor(S.ERROR, S.CLOSED, actor)
        assert_transition_allowed(S.ERROR, S.CLOSED, actor)  # no raise

    @pytest.mark.parametrize("actor", [A.STRATEGY, A.RISK_MANAGER, A.KILL_SWITCH])
    def test_error_to_closed_denied_for_automated_actors(self, actor):
        assert not is_transition_allowed_for_actor(S.ERROR, S.CLOSED, actor)
        with pytest.raises(IllegalStateTransitionError):
            assert_transition_allowed(S.ERROR, S.CLOSED, actor)

    def test_unrestricted_edges_allow_every_actor(self):
        for actor in _ALL_ACTORS:
            assert is_transition_allowed_for_actor(S.OPEN, S.EXIT_PENDING, actor)

    def test_illegal_edge_denied_regardless_of_actor(self):
        for actor in _ALL_ACTORS:
            assert not is_transition_allowed_for_actor(S.CLOSED, S.OPEN, actor)


class TestAssertRaises:
    def test_illegal_edge_raises_with_structured_fields(self):
        with pytest.raises(IllegalStateTransitionError) as exc_info:
            assert_transition_allowed(S.OPEN, S.CLOSED, A.STRATEGY)
        err = exc_info.value
        assert err.from_state is S.OPEN
        assert err.to_state is S.CLOSED
        assert err.actor is A.STRATEGY


# --------------------------------------------------------------------------
# PositionStateMachine.transition (against a fake repository)
# --------------------------------------------------------------------------


class TestTransitionPersistence:
    def test_legal_transition_delegates_to_repository_and_returns_row(self):
        repo = FakeRepository()
        sm = PositionStateMachine(repo)
        position = FakePosition(S.ENTRY_PENDING)

        result = sm.transition(
            position, to_state=S.OPEN, actor=A.STRATEGY, reason="both legs filled"
        )

        assert result is not None
        assert position.state is S.OPEN
        assert len(repo.calls) == 1
        assert repo.calls[0].from_state is S.ENTRY_PENDING
        assert repo.calls[0].to_state is S.OPEN
        assert repo.calls[0].reason == "both legs filled"

    def test_illegal_transition_raises_and_writes_nothing(self):
        repo = FakeRepository()
        sm = PositionStateMachine(repo)
        position = FakePosition(S.OPEN)

        with pytest.raises(IllegalStateTransitionError):
            sm.transition(position, to_state=S.CLOSED, actor=A.STRATEGY)

        assert position.state is S.OPEN  # unchanged
        assert repo.calls == []  # nothing persisted

    def test_same_state_is_idempotent_noop(self):
        repo = FakeRepository()
        sm = PositionStateMachine(repo)
        position = FakePosition(S.OPEN)

        result = sm.transition(position, to_state=S.OPEN, actor=A.RECOVERY)

        assert result is None
        assert position.state is S.OPEN
        assert repo.calls == []  # no audit row for a no-op

    def test_full_happy_path_cycle(self):
        repo = FakeRepository()
        sm = PositionStateMachine(repo)
        position = FakePosition(S.IDLE)

        sm.transition(position, to_state=S.ENTRY_PENDING, actor=A.STRATEGY)
        sm.transition(position, to_state=S.OPEN, actor=A.STRATEGY)
        sm.transition(position, to_state=S.EXIT_PENDING, actor=A.STRATEGY)
        sm.transition(position, to_state=S.CLOSED, actor=A.STRATEGY)

        assert position.state is S.CLOSED
        assert [c.to_state for c in repo.calls] == [
            S.ENTRY_PENDING,
            S.OPEN,
            S.EXIT_PENDING,
            S.CLOSED,
        ]

    def test_clean_entry_abort_path(self):
        # Entry fired, no exposure ever opened (e.g. both legs rejected):
        # ENTRY_PENDING -> CLOSED is a legal clean close.
        repo = FakeRepository()
        sm = PositionStateMachine(repo)
        position = FakePosition(S.ENTRY_PENDING)

        sm.transition(position, to_state=S.CLOSED, actor=A.STRATEGY, reason="both legs rejected")

        assert position.state is S.CLOSED


class TestMarkError:
    @pytest.mark.parametrize(
        "start", [S.IDLE, S.ENTRY_PENDING, S.OPEN, S.EXIT_PENDING]
    )
    def test_mark_error_from_every_non_terminal_state(self, start):
        repo = FakeRepository()
        sm = PositionStateMachine(repo)
        position = FakePosition(start)

        sm.mark_error(position, actor=A.RISK_MANAGER, reason="broker disconnect")

        assert position.state is S.ERROR
        assert repo.calls[-1].to_state is S.ERROR
        assert repo.calls[-1].reason == "broker disconnect"

    def test_mark_error_when_already_error_is_noop(self):
        repo = FakeRepository()
        sm = PositionStateMachine(repo)
        position = FakePosition(S.ERROR)

        result = sm.mark_error(position, actor=A.RECOVERY, reason="re-detected")

        assert result is None
        assert repo.calls == []

    def test_mark_error_on_closed_position_raises(self):
        repo = FakeRepository()
        sm = PositionStateMachine(repo)
        position = FakePosition(S.CLOSED)

        with pytest.raises(IllegalStateTransitionError):
            sm.mark_error(position, actor=A.MANUAL, reason="too late")

        assert position.state is S.CLOSED
        assert repo.calls == []


class TestErrorResolution:
    def test_recovery_can_resolve_error_to_closed(self):
        repo = FakeRepository()
        sm = PositionStateMachine(repo)
        position = FakePosition(S.ERROR)

        sm.transition(position, to_state=S.CLOSED, actor=A.RECOVERY, reason="squared off")

        assert position.state is S.CLOSED

    def test_strategy_cannot_resolve_error_to_closed(self):
        repo = FakeRepository()
        sm = PositionStateMachine(repo)
        position = FakePosition(S.ERROR)

        with pytest.raises(IllegalStateTransitionError):
            sm.transition(position, to_state=S.CLOSED, actor=A.STRATEGY)

        assert position.state is S.ERROR
        assert repo.calls == []


class TestRecoverySupport:
    def test_current_state_reads_from_the_position_row(self):
        # "Restart recovery" at the state-machine level is exactly this: the
        # persisted row is authoritative, never an assumed IDLE.
        for state in S:
            assert PositionStateMachine.current_state(FakePosition(state)) is state
