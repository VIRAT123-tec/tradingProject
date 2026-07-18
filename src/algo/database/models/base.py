"""Shared declarative base and mixins for every ORM model in algo_platform.

Two things live here that every model depends on:

1. ``Base`` -- the SQLAlchemy 2.0 declarative base, configured with an explicit
   constraint-naming convention. Without this, Alembic autogenerate produces
   unpredictable, backend-specific constraint names (e.g. Postgres invents its
   own names for unnamed CHECK/UNIQUE constraints), which makes future
   migrations that need to reference a constraint by name fragile. Naming
   every constraint deterministically from table/column names means Alembic
   diffs are stable and reviewable.

2. ``TimestampMixin`` -- adds ``created_at``/``updated_at`` columns using
   timezone-aware UTC timestamps, consistently across every table, so no
   individual model has to make its own (easy to get wrong) timestamp decision.
"""

from datetime import datetime

from sqlalchemy import DateTime, MetaData, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    """Declarative base for all algo_platform ORM models.

    All model modules under database/models/ must subclass this ``Base``
    (directly or via TimestampMixin) so that every table shares one
    ``MetaData`` object -- this is what lets Alembic's ``env.py`` discover the
    full schema from a single import (``from algo.database.models import Base``)
    and autogenerate migrations covering every table at once.
    """

    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class TimestampMixin:
    """Adds created_at/updated_at timestamptz columns, maintained by the database
    itself (``server_default``/``onupdate``) rather than by application code, so
    the timestamp is trustworthy even if a caller forgets to set it.
    """

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        doc="Row creation time (UTC, database-assigned).",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        doc="Row last-modified time (UTC, database-assigned).",
    )
