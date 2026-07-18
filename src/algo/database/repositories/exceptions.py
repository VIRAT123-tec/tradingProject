"""Repository-layer exceptions.

Repositories translate SQLAlchemy-internal exceptions (StaleDataError,
IntegrityError) into these types so callers -- services, strategy_engine,
execution -- depend only on this module's vocabulary, never on SQLAlchemy
internals leaking through the persistence-layer boundary.
"""

from __future__ import annotations


class RepositoryError(Exception):
    """Base class for all repository-layer exceptions."""


class NotFoundError(RepositoryError):
    """Raised by a ``..._or_raise`` lookup when the requested row does not exist."""


class ConcurrentModificationError(RepositoryError):
    """Raised when an optimistic-lock version check fails on update.

    This means another writer modified the row between when it was loaded
    and when this update was flushed -- e.g. the independent kill-switch
    watcher and the strategy's own exit_logic both attempting to transition
    the same Position. The repository does not retry automatically: retrying
    is a business decision (re-evaluate from fresh state, or defer to
    whichever writer won), not a persistence one.
    """
