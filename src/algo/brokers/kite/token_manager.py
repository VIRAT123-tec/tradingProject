"""Orchestrates the daily Kite access-token lifecycle: detect whether today's
token is still usable, and if not, drive the official interactive login flow
to mint a fresh one.

Deliberately separate from ``kite_auth.KiteSession`` (not a replacement for
it): ``KiteSession`` is what the *running trading process* uses to apply an
already-minted token and fail cleanly if none exists -- it must never itself
open a browser or block on input. ``TokenManager`` is the higher-level daily
operator flow (driven by ``scripts/generate_token.py``) that decides *whether*
a fresh login is needed and, if so, obtains a ``request_token`` through an
injected callback and hands it to a ``KiteSession`` for the actual exchange --
reusing that already-tested code path rather than duplicating it.

The interactive part (opening a browser, prompting for the redirect) is
intentionally not implemented here: it is injected as a plain callable
``(login_url: str) -> request_token: str``. That keeps this module free of any
UI/IO concerns, so the entire login-required-vs-not decision, the local
same-day freshness check, and the live profile-API validation are all
unit-testable with a fake callback and a fake Kite client -- no real browser,
no real network, no real credentials.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Callable

from algo.brokers.exceptions import BrokerAuthenticationError
from algo.brokers.kite.token_store import TokenRecord

if TYPE_CHECKING:
    from algo.brokers.kite.kite_auth import KiteSession
    from algo.brokers.kite.kite_broker import KiteClientProtocol
    from algo.brokers.kite.token_store import TokenStore
    from algo.strategy_engine.strategy_context import TimeProvider

__all__ = ["TokenManager"]

#: Given the Kite login URL, obtain and return a request_token (or a full
#: redirect URL containing one -- see ``generate_token.py``'s parsing). This is
#: where the actual interactive step (open a browser, prompt for input) lives;
#: TokenManager never performs I/O itself.
LoginCallback = Callable[[str], str]


class TokenManager:
    """Ensures a valid Kite access token is available, running the official
    interactive login flow only when one genuinely is not.
    """

    def __init__(
        self,
        *,
        client: KiteClientProtocol,
        session: KiteSession,
        token_store: TokenStore,
        time_provider: TimeProvider,
        logger: logging.Logger | None = None,
    ) -> None:
        """``client`` is used directly only for ``login_url()`` and the
        validation ``profile()`` call; the actual token exchange is delegated
        to ``session`` (a ``KiteSession`` already wired to the same client, api
        secret, and ``token_store``), so that proven exchange/error-translation
        logic is never duplicated here.
        """
        self._client = client
        self._session = session
        self._token_store = token_store
        self._time = time_provider
        self._logger = logger if logger is not None else logging.getLogger("algo.brokers.kite.token_manager")

    def login_url(self) -> str:
        """The official Kite Connect login URL the user must open in a
        browser to authenticate interactively (per Kite's own flow -- this
        platform never collects or stores a password or TOTP secret)."""
        return self._client.login_url()

    def check_existing(self) -> str | None:
        """Return a currently usable access token, or ``None`` if a fresh
        login is required.

        This is the "automatic expiry detection" step, in two layers:

        1. If a stored :class:`~algo.brokers.kite.token_store.TokenRecord`
           exists and was generated on a *different* IST calendar day than
           today, it is treated as expired without any network call --  Kite
           invalidates every session once per day, so a stale generation date
           already answers the question.
        2. Otherwise (generated today, or no generation-time record exists at
           all -- e.g. a token was placed by some other means), the token is
           validated live via the broker's ``profile()`` call, which is the
           only way to catch a token revoked mid-day (an admin action, a
           manual logout) that a stored date could never reveal.

        Never raises: any failure (network error, malformed record, invalid
        token) simply means "no usable token," which the caller resolves by
        triggering a fresh login.
        """
        record = self._token_store.get_token_record()
        if record is not None:
            if self._is_from_today(record.generated_at) and self._validate(record.access_token):
                self._logger.info("existing Kite access token is valid for today")
                return record.access_token
            self._logger.info(
                "stored Kite token record is stale or failed validation; a fresh login is required"
            )
            return None

        # No generation-time record -- fall back to asking the broker
        # directly, rather than assuming an unknown-age token is stale.
        bare_token = self._token_store.get_access_token()
        if bare_token and self._validate(bare_token):
            self._logger.info(
                "found a Kite access token with no recorded generation time, but it "
                "validated live; treating it as usable for today"
            )
            return bare_token
        return None

    def exchange_request_token(self, request_token: str) -> str:
        """Exchange a one-time ``request_token`` (from the login redirect) for
        the day's access token via the official Kite API, and durably persist
        it together with the generation time.

        Delegates the actual exchange call to the injected ``KiteSession``
        (already tested, already translates Kite's exceptions correctly), then
        records the generation time on top so future :meth:`check_existing`
        calls can detect same-day expiry without a network call.
        """
        access_token = self._session.generate_session(request_token)
        record = TokenRecord(access_token=access_token, generated_at=self._time.now_ist())
        self._token_store.save_token(record)
        self._logger.info("new Kite access token generated and stored")
        return access_token

    def ensure_valid_token(self, *, on_login_required: LoginCallback | None = None) -> str:
        """The main entry point: return a valid access token, running the
        official interactive login flow if (and only if) one is required.

        If :meth:`check_existing` finds a usable token, it is returned
        immediately -- no login prompt, no browser. Otherwise, if
        ``on_login_required`` was supplied, it is called with the login URL to
        obtain a ``request_token`` (the interactive step -- opening a browser
        and reading the redirect), which is then exchanged and validated.

        Raises ``BrokerAuthenticationError`` if a login is required but
        ``on_login_required`` is ``None`` -- a non-interactive caller (e.g. the
        running trading process) must never silently open a browser or block
        on input; it should fail loudly and direct the operator to
        ``scripts/generate_token.py`` instead.
        """
        existing = self.check_existing()
        if existing is not None:
            return existing

        if on_login_required is None:
            raise BrokerAuthenticationError(
                "no valid Kite access token is available and no interactive login "
                "handler was provided; run scripts/generate_token.py to log in"
            )

        request_token = on_login_required(self.login_url())
        access_token = self.exchange_request_token(request_token)

        if not self._validate(access_token):
            raise BrokerAuthenticationError(
                "the newly generated Kite access token failed profile validation "
                "immediately after login -- this should not normally happen; verify "
                "KITE_API_KEY/KITE_API_SECRET are correct and try again"
            )
        self._logger.info("Kite access token validated successfully against the profile API")
        return access_token

    # -- Internal ------------------------------------------------------

    def _is_from_today(self, generated_at: datetime) -> bool:
        return generated_at.date() == self._time.today()

    def _validate(self, access_token: str) -> bool:
        """Apply ``access_token`` to the client and confirm it works by
        calling the broker's ``profile()`` API -- the literal "validate the
        generated token by calling the broker profile API" requirement. Any
        failure (an expired/invalid token, or a transient network error) is
        logged and treated as invalid; this is a boolean freshness check, not
        a call whose failure should propagate as a typed broker exception.
        """
        try:
            self._client.set_access_token(access_token)
            self._client.profile()
        except Exception:  # noqa: BLE001 -- deliberately broad: any failure means "not valid"
            self._logger.debug("Kite token validation call failed", exc_info=True)
            return False
        return True
