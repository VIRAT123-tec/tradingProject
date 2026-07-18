"""Single place that decides how Python enums are stored in the database.

Every enum column in this schema is built through ``enum_column()`` so the
storage decision -- validated string with a CHECK constraint, not a native
Postgres ENUM type -- is made once, not re-decided (and potentially
re-litigated inconsistently) in every model file. See common/enums.py's module
docstring for the full rationale: native Postgres ENUM types make adding a new
member an ``ALTER TYPE`` migration with awkward transactional restrictions;
a VARCHAR + CHECK constraint makes it a plain metadata change.
"""

from enum import Enum
from typing import TypeVar

from sqlalchemy import Enum as SAEnum

E = TypeVar("E", bound=Enum)


def enum_column(enum_cls: type[E], length: int = 32, name: str | None = None) -> SAEnum:
    """Build the SQLAlchemy column type for a str-Enum, stored as a validated
    VARCHAR rather than a native database enum type.

    ``length`` defaults to 32, comfortably longer than any current member name
    (the longest today is SUBMITTED_UNCONFIRMED at 22 chars) so a future enum
    addition with a longer name does not itself require a column-width migration.

    ``name`` defaults to a name derived from the enum class alone (e.g.
    ``exchange_enum``), which is only safe when a table uses a given enum type
    on a single column. When a table reuses the same enum type on two
    different columns -- e.g. ``position_state_transitions.from_state`` and
    ``.to_state``, both ``PositionState`` -- pass an explicit, column-specific
    ``name`` (e.g. ``"positionstate_from_enum"`` / ``"positionstate_to_enum"``)
    for each. Otherwise the two auto-generated CHECK constraints collide on
    name within that table (``ck_<table>_<enum_name>``, per this project's
    naming convention) and the migration fails against Postgres with a
    duplicate-constraint error -- exactly what happened before
    ``position_state_transitions`` was fixed to pass distinct names.
    """

    return SAEnum(
        enum_cls,
        name=name or f"{enum_cls.__name__.lower()}_enum",
        native_enum=False,
        validate_strings=True,
        length=length,
        create_constraint=True,
    )
