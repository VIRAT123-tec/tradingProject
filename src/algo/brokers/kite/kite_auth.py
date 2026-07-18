"""Kite Connect login/session generation and daily access-token lifecycle.

Kite's auth model, and how it shapes this module:

* There is no auto-refreshing OAuth token. A KiteConnect ``access_token`` is
  minted once per trading day through an interactive login (the user opens
  ``login_url()``, logs in, and Kite redirects back with a one-time
  ``request_token``, which is exchanged via ``generate_session`` for the day's
  ``access_token``). That interactive step is driven by
  ``scripts/generate_token.py``, not by the running platform.
* The daily token is then handed to the platform through an injected
  ``AccessTokenStore`` (a file, an env var, a secrets manager -- this module
  does not care which). ``KiteSession.activate`` loads it and applies it to the
  client.
* The token simply expires (end of day, or on an admin logout). There is
  nothing to "refresh": an expired token surfaces as a ``TokenException`` from
  any API call, which the broker maps to a non-retryable
  ``BrokerAuthenticationError``. Recovery is a fresh interactive login, never an
  automatic retry -- treating token expiry as retryable is exactly the kind of
  silent-failure mode a real-money system must avoid.

Everything here is dependency-injected -- the KiteConnect client and the token
store are both passed in -- so the whole flow is unit-testable with fakes,
without any network or real credentials.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

from algo.brokers.exceptions import BrokerAuthenticationError
from algo.brokers.kite.mapper import translate_kite_exception

if TYPE_CHECKING:
    from algo.brokers.kite.kite_broker import KiteClientProtocol


@runtime_checkable
class AccessTokenStore(Protocol):
    """Seam for reading (and, after a login, writing) the day's Kite access
    token. A concrete implementation might back onto a file written by
    ``generate_token.py``, an environment variable, or a secrets manager -- the
    session does not depend on which."""

    def get_access_token(self) -> str | None:
        """The current day's access token, or None if no valid token is
        available (never logged in today, or it was cleared)."""
        ...

    def set_access_token(self, access_token: str) -> None:
        """Persist a freshly generated access token (called by the login flow
        after a successful ``generate_session``)."""
        ...


class EnvAccessTokenStore:
    """A concrete ``AccessTokenStore`` backed by an environment variable
    (default ``KITE_ACCESS_TOKEN``).

    The trading process reads the day's token from the environment; the daily
    interactive login flow (``scripts/generate_token.py``) is responsible for
    populating that environment (e.g. writing the ``.env`` file or a secret the
    deployment injects) before the trading process starts. ``set_access_token``
    updates the value in the current process only -- it cannot persist a value
    into a *future* process's environment, which is by design: minting and
    durably storing the token is the login flow's job, not the trading
    process's.
    """

    def __init__(self, env_var: str = "KITE_ACCESS_TOKEN") -> None:
        self._env_var = env_var

    def get_access_token(self) -> str | None:
        import os

        value = os.environ.get(self._env_var)
        return value or None

    def set_access_token(self, access_token: str) -> None:
        import os

        os.environ[self._env_var] = access_token


class KiteSession:
    """Owns the Kite access-token lifecycle for one KiteConnect client.

    Injected with the client, the API secret, and an ``AccessTokenStore``. Does
    no network I/O of its own beyond the two Kite calls it wraps
    (``generate_session`` for the interactive login exchange, and applying the
    token via ``set_access_token``, which is local).
    """

    def __init__(
        self,
        *,
        client: KiteClientProtocol,
        api_secret: str,
        token_store: AccessTokenStore,
    ) -> None:
        self._client = client
        self._api_secret = api_secret
        self._token_store = token_store
        self._active = False

    @property
    def is_active(self) -> bool:
        """Whether a token has been applied to the client this session (a cheap
        local flag, not a liveness check -- the token may still have expired
        broker-side; only an API call reveals that)."""
        return self._active

    def activate(self) -> None:
        """Load the current access token from the store and apply it to the
        client, so subsequent API calls are authenticated.

        Raises ``BrokerAuthenticationError`` if no token is available -- there
        is nothing to fall back to, and proceeding unauthenticated would only
        produce a more confusing failure at the first API call.
        """
        token = self._token_store.get_access_token()
        if not token:
            self._active = False
            raise BrokerAuthenticationError(
                "no Kite access token available; run the daily login flow "
                "(scripts/generate_token.py) to mint one"
            )
        self._client.set_access_token(token)
        self._active = True

    def invalidate(self) -> None:
        """Mark the session inactive (e.g. after a TokenException). Does not
        clear the stored token -- deciding whether the stored token is bad or
        the failure was transient is the operator's call at re-login time."""
        self._active = False

    def generate_session(self, request_token: str) -> str:
        """Exchange a one-time ``request_token`` (from the interactive login
        redirect) for the day's access token, apply it to the client, and
        persist it via the token store. Returns the new access token.

        This is the only method that performs the login network round-trip; it
        is called by ``scripts/generate_token.py``, never by the running trading
        process. Failures are translated to the platform's ``BrokerError``
        hierarchy (a bad request token surfaces as an auth/input error).
        """
        try:
            data = self._client.generate_session(request_token, self._api_secret)
        except Exception as exc:  # noqa: BLE001 -- translated immediately below
            raise translate_kite_exception(exc, mutating=False) from exc
        access_token = data.get("access_token")
        if not access_token:
            raise BrokerAuthenticationError(
                # Log only the response *keys*, never the values -- the payload
                # can carry sensitive session fields that must not reach logs.
                "Kite generate_session returned no access_token "
                f"(response keys: {sorted(data)})"
            )
        self._client.set_access_token(access_token)
        self._token_store.set_access_token(access_token)
        self._active = True
        return access_token
