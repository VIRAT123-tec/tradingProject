"""Session factory and transaction-scope management.

A SQLAlchemy Session is explicitly *not* thread-safe -- each unit of work
must get its own Session instance. This module never exposes a single shared
Session; it exposes a sessionmaker (itself safe to share across threads,
since every call to it produces an independent Session) plus a
session_scope() context manager that gives callers correct commit/rollback/
close semantics without repeating that boilerplate at every call site.

Deliberately not using SQLAlchemy's scoped_session (a thread-local "current
session" registry): the repository layer (database/repositories/) already
takes an explicit Session via constructor injection, so an implicit
thread-local session would add a second, inconsistent way of obtaining a
session alongside the explicit one, without buying anything -- explicit
construction is both easier to reason about and easier to unit test.

Usage note: do not nest calls to session_scope() -- each call opens an
independent transaction against an independent Session, so nesting them does
not make an inner operation part of the outer transaction. A function that
needs to participate in an already-open transaction should receive that
Session as a parameter instead of opening its own scope.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from algo.database.database import get_engine


def build_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Construct a new sessionmaker bound to the given engine.

    Pure function: no caching, no global state. Exercised directly in tests
    against a test engine, independent of the process-wide singleton below.

    expire_on_commit=False: strategy/execution/risk code frequently needs to
    read an object's attributes (e.g. a just-created Position's id or state)
    after the session_scope() block that committed it has already exited and
    closed the Session. With the default expire_on_commit=True, that read
    would raise DetachedInstanceError, since the object's attributes are
    expired at commit and a fresh SELECT is no longer possible without an
    open Session. With it False, attributes remain readable using their
    value as of the commit -- callers that need a guaranteed-fresh read
    afterward should re-fetch the row in a new session_scope() instead of
    reusing a detached object.
    """
    return sessionmaker(bind=engine, autoflush=True, expire_on_commit=False)


_session_factory: sessionmaker[Session] | None = None
_session_factory_lock = threading.Lock()


def get_session_factory() -> sessionmaker[Session]:
    """Return the process-wide sessionmaker singleton, building it (from the
    process-wide Engine singleton) on first call. Thread-safe via
    double-checked locking, matching database.get_engine()'s pattern.
    """
    global _session_factory
    if _session_factory is None:
        with _session_factory_lock:
            if _session_factory is None:
                _session_factory = build_session_factory(get_engine())
    return _session_factory


def reset_session_factory() -> None:
    """Clear the cached sessionmaker singleton, so the next
    get_session_factory() call rebuilds it. Paired with
    database.dispose_engine() in test teardown."""
    global _session_factory
    with _session_factory_lock:
        _session_factory = None


@contextmanager
def unit_of_work(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    """One unit of work against an *explicitly provided* sessionmaker: commit
    on success, roll back and re-raise on error, always close the Session
    (returning its connection to the pool).

    This is the single implementation of that transaction pattern for the
    dependency-injected factories used throughout the strategy, risk, and
    service layers -- each of those components exposes a thin ``_unit_of_work``
    that delegates here rather than re-deriving the commit/rollback/close
    boilerplate. ``session_scope`` below is the same thing for the process-wide
    singleton factory.
    """
    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def session_scope() -> Iterator[Session]:
    """Provide one Session scoped to a single unit of work, using the
    process-wide sessionmaker singleton.

    Commits if the block completes without raising; rolls back and
    re-raises the original exception otherwise; always closes the Session
    (returning its connection to the pool) regardless of outcome.

    This is the transaction boundary the repository layer's own contract
    assumes: repositories flush() but never commit(), so multiple repository
    calls made within one `with session_scope() as session:` block land in
    exactly one atomic commit -- e.g. persisting a Position and its
    OrderIntent together, per the crash-recovery design.

    Not every caller should use this: a long-running process (e.g. a
    strategy instance's monitoring loop spanning many ticks) may need finer
    control over exactly when a transaction boundary closes than "the whole
    block." Such callers should call get_session_factory() directly and
    manage commit/rollback/close themselves.
    """
    with unit_of_work(get_session_factory()) as session:
        yield session
