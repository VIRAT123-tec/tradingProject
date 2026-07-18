"""Generic base repository: the shared persistence machinery every concrete
repository builds on.

Three things live here so they are implemented once, correctly, rather than
re-derived (and potentially re-derived inconsistently) in every repository:

1. Plain by-id lookups, including a ``FOR UPDATE`` variant for the row-lock
   pattern the database architecture calls for when a Position (or similar)
   could be concurrently touched by the independent kill switch and the
   strategy's own logic.
2. A SAVEPOINT-based ``get_or_create`` helper for idempotent creation
   (Position per trading day, OrderIntent per idempotency key, ...). Without
   the SAVEPOINT, catching the IntegrityError from a duplicate insert would
   still leave the caller's outer transaction aborted on Postgres -- Postgres
   fails the *entire* transaction on any statement error, unlike some other
   databases, so the failed insert must be contained in a nested transaction
   (SAVEPOINT) that can be rolled back on its own.
3. Translation of SQLAlchemy's StaleDataError (an optimistic-lock version
   mismatch) into this package's ConcurrentModificationError, so callers
   never need to know SQLAlchemy's exception vocabulary.

Transaction ownership contract: every method here calls ``flush()``, never
``commit()``. Flushing sends pending SQL to the database (assigning
primary keys, running constraint checks) without ending the transaction --
the caller (a service function, or later entry_logic.py/exit_logic.py) owns
the transaction boundary via the Session it injected, and is free to combine
several repository calls into one atomic commit.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Generic, TypeVar

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import StaleDataError

from algo.database.models.base import Base
from algo.database.repositories.exceptions import ConcurrentModificationError, NotFoundError

ModelT = TypeVar("ModelT", bound=Base)


class BaseRepository(Generic[ModelT]):
    """Shared persistence operations, parameterized by the concrete ORM model.

    Every concrete model in this schema has an integer primary key column
    named exactly ``id`` (verified when the models were built) -- this base
    class relies on that convention rather than re-declaring it per subclass.
    """

    model: type[ModelT]

    def __init__(self, session: Session) -> None:
        self.session = session

    def get_by_id(self, id_: int) -> ModelT | None:
        """Fetch by primary key, or None if no such row exists."""
        return self.session.get(self.model, id_)

    def get_by_id_or_raise(self, id_: int) -> ModelT:
        """Fetch by primary key, raising NotFoundError if it does not exist --
        for call sites where the row's existence is already assumed (e.g.
        recovering a position whose id came from our own prior query)."""
        obj = self.get_by_id(id_)
        if obj is None:
            raise NotFoundError(f"{self.model.__name__} with id={id_} not found")
        return obj

    def get_by_id_for_update(self, id_: int) -> ModelT | None:
        """Fetch by primary key with a row lock (SELECT ... FOR UPDATE).

        Use this instead of get_by_id whenever the caller is about to mutate
        the row within the current transaction and another writer touching
        the same row concurrently must wait rather than race -- e.g. loading
        a Position immediately before a state transition, given the
        independent kill switch can attempt the same transition concurrently.
        """
        stmt = select(self.model).where(self.model.id == id_).with_for_update()
        return self.session.execute(stmt).scalar_one_or_none()

    def add(self, obj: ModelT) -> ModelT:
        """Stage a new row and flush it (assigning its primary key)."""
        self.session.add(obj)
        self.flush()
        return obj

    def flush(self) -> None:
        """Flush pending changes, translating a StaleDataError (optimistic-
        lock version mismatch) into ConcurrentModificationError."""
        try:
            self.session.flush()
        except StaleDataError as exc:
            raise ConcurrentModificationError(
                f"{self.model.__name__} row was concurrently modified "
                "(optimistic lock version mismatch); reload and retry."
            ) from exc

    def _get_or_create(
        self,
        *,
        lookup: Callable[[], ModelT | None],
        factory: Callable[[], ModelT],
    ) -> tuple[ModelT, bool]:
        """Idempotent create: look up first, and only if absent, insert inside
        a SAVEPOINT so a concurrent duplicate insert (e.g. a scheduler misfire
        racing this same call) is resolved by re-querying rather than by
        poisoning the caller's outer transaction.

        Returns (row, created) -- created=False means a concurrent writer (or
        the initial lookup) already had this row; the caller's factory()
        values were not applied in that case.
        """
        existing = lookup()
        if existing is not None:
            return existing, False

        try:
            with self.session.begin_nested():
                obj = factory()
                self.session.add(obj)
                self.session.flush()
        except IntegrityError:
            existing = lookup()
            if existing is None:
                # The IntegrityError was not the expected unique-constraint
                # race (lookup still finds nothing) -- a genuine, unexpected
                # persistence failure. Propagate rather than mask it.
                raise
            return existing, False

        return obj, True
