"""Durable, disk-persisted storage for the daily Kite access token.

Why this exists alongside ``kite_auth.EnvAccessTokenStore``: that store's own
docstring is explicit that ``set_access_token`` only updates *its own
process's* environment -- it cannot hand a freshly minted token to a *later*
process, because nothing was ever written to disk. That is exactly the gap
between "the interactive login script just ran" and "the trading process
starts tomorrow morning and needs today's token": two different process
invocations. ``EnvFileTokenStore`` closes that gap by reading and writing the
``.env`` file itself (via ``python-dotenv``), so a token minted by
``scripts/generate_token.py`` is still there for a trading process that starts
later, including on a full machine restart.

It also stores *when* the token was generated (as a second env var), which is
what lets ``token_manager.py`` detect same-day expiry without a network call --
Kite invalidates every session once per day, so "generated on a different
calendar day (IST)" already implies "expired," and asking the broker to
confirm that would be a wasted round trip. Live validation (a real
``profile()`` call) is used for what a stored date can *not* tell you: a token
revoked mid-day by an admin action.

Everything here is dependency-injected and does no network I/O of its own, so
it is fully unit-testable against a temporary file, with no real ``.env`` and
no Kite credentials.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol, runtime_checkable

from dotenv import dotenv_values, find_dotenv, set_key

from algo.brokers.kite.kite_auth import AccessTokenStore

__all__ = ["TokenRecord", "TokenStore", "EnvFileTokenStore"]

_DEFAULT_TOKEN_VAR = "KITE_ACCESS_TOKEN"
_DEFAULT_GENERATED_AT_VAR = "KITE_ACCESS_TOKEN_GENERATED_AT"


@dataclass(frozen=True, slots=True)
class TokenRecord:
    """A Kite access token plus the instant it was generated.

    ``generated_at`` is expected to be timezone-aware (IST, per this
    platform's convention of never producing a naive datetime) -- it is what
    ``TokenManager`` compares against "today" to detect same-day expiry
    without calling the broker.
    """

    access_token: str
    generated_at: datetime

    def __post_init__(self) -> None:
        if not self.access_token:
            raise ValueError("TokenRecord.access_token must be a non-empty string")
        if self.generated_at.tzinfo is None:
            raise ValueError(
                "TokenRecord.generated_at must be timezone-aware "
                "(use TimeProvider.now_ist(), never a naive datetime)"
            )


@runtime_checkable
class TokenStore(AccessTokenStore, Protocol):
    """``AccessTokenStore`` (the seam ``KiteSession`` already depends on) plus
    generation-time metadata, needed for same-day expiry detection.

    Extending rather than replacing ``AccessTokenStore`` means any
    ``TokenStore`` is also a drop-in for ``KiteSession``'s existing
    constructor -- no change to that already-approved code is required for a
    ``TokenStore`` to work there.
    """

    def get_token_record(self) -> TokenRecord | None:
        """The current token plus when it was generated, or ``None`` if no
        record is available (never logged in, or the record is malformed)."""
        ...

    def save_token(self, record: TokenRecord) -> None:
        """Durably persist a freshly generated token and its generation time."""
        ...


class EnvFileTokenStore:
    """A ``TokenStore`` backed by a ``.env`` file on disk.

    Reads and writes go straight to the file (via ``python-dotenv``), which is
    what makes the token durable across process restarts -- the defining
    difference from ``kite_auth.EnvAccessTokenStore``. Every write also mirrors
    into ``os.environ`` so the *current* process sees the new value
    immediately, matching how the rest of the platform reads Kite credentials.

    Pure file I/O, no network calls, no shell dependency -- works identically
    on Windows, macOS, and Linux.
    """

    def __init__(
        self,
        *,
        env_path: Path | None = None,
        token_var: str = _DEFAULT_TOKEN_VAR,
        generated_at_var: str = _DEFAULT_GENERATED_AT_VAR,
    ) -> None:
        """``env_path`` defaults to the ``.env`` discovered via
        ``python-dotenv``'s own search (walking up from the current working
        directory, matching how ``database.py`` already resolves it), falling
        back to ``./.env`` if none is found. The file is created if it does
        not exist yet, so the first-ever token write never fails on a missing
        file.
        """
        self._env_path = env_path if env_path is not None else self._resolve_default_env_path()
        self._token_var = token_var
        self._generated_at_var = generated_at_var
        self._env_path.parent.mkdir(parents=True, exist_ok=True)
        self._env_path.touch(exist_ok=True)

    @staticmethod
    def _resolve_default_env_path() -> Path:
        found = find_dotenv(usecwd=True)
        return Path(found) if found else Path(".env")

    @property
    def env_path(self) -> Path:
        """The ``.env`` file this store reads from and writes to."""
        return self._env_path

    # -- AccessTokenStore (satisfies KiteSession's existing seam) ------

    def get_access_token(self) -> str | None:
        """The current day's access token from the ``.env`` file, or ``None``
        if unset. Reads the file directly (not ``os.environ``), so this always
        reflects the latest value on disk, even if it was written by a
        different process."""
        value = dotenv_values(self._env_path).get(self._token_var)
        return value or None

    def set_access_token(self, access_token: str) -> None:
        """Persist a token (no generation-time metadata). Prefer
        :meth:`save_token` when the caller knows when the token was minted --
        this method exists to satisfy ``AccessTokenStore`` for callers (like
        ``KiteSession.generate_session``) that only deal in the bare token."""
        self._write(self._token_var, access_token)

    # -- TokenStore (generation-time metadata) --------------------------

    def get_token_record(self) -> TokenRecord | None:
        """The stored token and its generation time, or ``None`` if either is
        missing or the generation time cannot be parsed. A malformed or absent
        timestamp is deliberately treated as "no record" (never guessed) --
        the caller falls back to a live validation instead of trusting an
        unknown-age token."""
        values = dotenv_values(self._env_path)
        token = values.get(self._token_var)
        raw_generated_at = values.get(self._generated_at_var)
        if not token or not raw_generated_at:
            return None
        try:
            generated_at = datetime.fromisoformat(raw_generated_at)
        except ValueError:
            return None
        if generated_at.tzinfo is None:
            return None
        return TokenRecord(access_token=token, generated_at=generated_at)

    def save_token(self, record: TokenRecord) -> None:
        """Durably persist both the token and its generation time."""
        self._write(self._token_var, record.access_token)
        self._write(self._generated_at_var, record.generated_at.isoformat())

    # -- Internal --------------------------------------------------------

    def _write(self, var_name: str, value: str) -> None:
        set_key(str(self._env_path), var_name, value)
        os.environ[var_name] = value
