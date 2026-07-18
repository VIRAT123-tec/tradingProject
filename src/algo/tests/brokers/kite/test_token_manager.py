"""Tests for TokenManager: the same-day expiry decision, the login-required
handoff, and post-login validation.

Everything is driven through fakes (a fake Kite client, a fake KiteSession,
a fake TokenStore, a fake TimeProvider, and a fake ``on_login_required``
callback) so the whole daily-login decision tree is exercised with no
network, no browser, and no real Kite credentials.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

import pytest

from algo.brokers.exceptions import BrokerAuthenticationError
from algo.brokers.kite.token_manager import TokenManager
from algo.brokers.kite.token_store import TokenRecord

_IST = timezone(timedelta(hours=5, minutes=30))


class _Raise:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc


@dataclass
class FakeClient:
    """Stand-in for kiteconnect.KiteConnect's subset TokenManager touches
    directly (login_url, set_access_token, profile -- the actual token
    exchange is delegated to FakeSession, not this client)."""

    login_url_result: str = "https://kite.zerodha.com/connect/login?api_key=xyz"
    profile_result: object = field(default_factory=dict)
    access_token: str | None = None

    def login_url(self) -> str:
        return self.login_url_result

    def set_access_token(self, access_token: str) -> None:
        self.access_token = access_token

    def profile(self):
        if isinstance(self.profile_result, _Raise):
            raise self.profile_result.exc
        return self.profile_result


@dataclass
class FakeSession:
    """Stand-in for KiteSession -- only the one method TokenManager calls."""

    generate_session_result: object = "NEW_TOKEN"
    requested_tokens: list[str] = field(default_factory=list)

    def generate_session(self, request_token: str) -> str:
        self.requested_tokens.append(request_token)
        if isinstance(self.generate_session_result, _Raise):
            raise self.generate_session_result.exc
        return self.generate_session_result


@dataclass
class DictTokenStore:
    """Stand-in for TokenStore, backed by plain attributes instead of a file."""

    token: str | None = None
    record: TokenRecord | None = None
    saved: list[TokenRecord] = field(default_factory=list)

    def get_access_token(self) -> str | None:
        return self.token

    def set_access_token(self, access_token: str) -> None:
        self.token = access_token

    def get_token_record(self) -> TokenRecord | None:
        return self.record

    def save_token(self, record: TokenRecord) -> None:
        self.saved.append(record)
        self.record = record
        self.token = record.access_token


@dataclass
class FakeTime:
    today_value: date = date(2026, 7, 9)

    def now(self) -> datetime:
        return datetime.combine(self.today_value, datetime.min.time(), tzinfo=timezone.utc)

    def now_ist(self) -> datetime:
        return datetime.combine(self.today_value, datetime.min.time(), tzinfo=_IST).replace(hour=9)

    def today(self) -> date:
        return self.today_value


def build_manager(*, client=None, session=None, store=None, time=None):
    client = client if client is not None else FakeClient()
    session = session if session is not None else FakeSession()
    store = store if store is not None else DictTokenStore()
    time = time if time is not None else FakeTime()
    manager = TokenManager(client=client, session=session, token_store=store, time_provider=time)
    return manager, client, session, store, time


# --------------------------------------------------------------------------
# login_url
# --------------------------------------------------------------------------


class TestLoginUrl:
    def test_delegates_to_client(self):
        manager, client, *_ = build_manager()
        assert manager.login_url() == client.login_url_result


# --------------------------------------------------------------------------
# check_existing
# --------------------------------------------------------------------------


class TestCheckExisting:
    def test_returns_token_when_record_is_from_today_and_valid(self):
        store = DictTokenStore(record=TokenRecord(access_token="TODAY", generated_at=datetime(2026, 7, 9, 9, 0, tzinfo=_IST)))
        manager, client, *_ = build_manager(store=store)

        assert manager.check_existing() == "TODAY"
        assert client.access_token == "TODAY"  # validation applied it

    def test_returns_none_when_record_is_from_a_previous_day(self):
        store = DictTokenStore(record=TokenRecord(access_token="STALE", generated_at=datetime(2026, 7, 8, 9, 0, tzinfo=_IST)))
        client = FakeClient(profile_result=_Raise(RuntimeError("should not be called")))
        manager, *_ = build_manager(store=store, client=client)

        assert manager.check_existing() is None

    def test_returns_none_when_same_day_record_fails_live_validation(self):
        store = DictTokenStore(record=TokenRecord(access_token="REVOKED", generated_at=datetime(2026, 7, 9, 9, 0, tzinfo=_IST)))
        client = FakeClient(profile_result=_Raise(RuntimeError("TokenException")))
        manager, *_ = build_manager(store=store, client=client)

        assert manager.check_existing() is None

    def test_falls_back_to_bare_token_when_no_record_and_validates_live(self):
        store = DictTokenStore(token="BARE_TOKEN", record=None)
        manager, client, *_ = build_manager(store=store)

        assert manager.check_existing() == "BARE_TOKEN"
        assert client.access_token == "BARE_TOKEN"

    def test_returns_none_when_bare_token_fails_validation(self):
        store = DictTokenStore(token="BAD_TOKEN", record=None)
        client = FakeClient(profile_result=_Raise(RuntimeError("TokenException")))
        manager, *_ = build_manager(store=store, client=client)

        assert manager.check_existing() is None

    def test_returns_none_when_nothing_stored(self):
        manager, *_ = build_manager(store=DictTokenStore(token=None, record=None))
        assert manager.check_existing() is None


# --------------------------------------------------------------------------
# exchange_request_token
# --------------------------------------------------------------------------


class TestExchangeRequestToken:
    def test_exchanges_and_persists_record_with_current_time(self):
        session = FakeSession(generate_session_result="FRESH_TOKEN")
        store = DictTokenStore()
        time = FakeTime(today_value=date(2026, 7, 9))
        manager, _, session, store, _ = build_manager(session=session, store=store, time=time)

        token = manager.exchange_request_token("req_tok_abc")

        assert token == "FRESH_TOKEN"
        assert session.requested_tokens == ["req_tok_abc"]
        assert len(store.saved) == 1
        assert store.saved[0].access_token == "FRESH_TOKEN"
        assert store.saved[0].generated_at == time.now_ist()

    def test_propagates_session_exchange_failure(self):
        session = FakeSession(generate_session_result=_Raise(BrokerAuthenticationError("bad request token")))
        manager, *_ = build_manager(session=session)

        with pytest.raises(BrokerAuthenticationError):
            manager.exchange_request_token("bad_token")


# --------------------------------------------------------------------------
# ensure_valid_token
# --------------------------------------------------------------------------


class TestEnsureValidToken:
    def test_returns_existing_token_without_prompting_for_login(self):
        store = DictTokenStore(record=TokenRecord(access_token="TODAY", generated_at=datetime(2026, 7, 9, 9, 0, tzinfo=_IST)))
        manager, *_ = build_manager(store=store)

        calls: list[str] = []
        token = manager.ensure_valid_token(on_login_required=lambda url: calls.append(url) or "unused")

        assert token == "TODAY"
        assert calls == []

    def test_raises_when_login_required_but_no_callback_given(self):
        manager, *_ = build_manager(store=DictTokenStore())
        with pytest.raises(BrokerAuthenticationError):
            manager.ensure_valid_token(on_login_required=None)

    def test_runs_login_flow_and_returns_new_token_when_none_exists(self):
        client = FakeClient(login_url_result="https://kite.example/login")
        session = FakeSession(generate_session_result="BRAND_NEW")
        manager, *_ = build_manager(client=client, session=session, store=DictTokenStore())

        seen_urls: list[str] = []

        def fake_login(url: str) -> str:
            seen_urls.append(url)
            return "the_request_token"

        token = manager.ensure_valid_token(on_login_required=fake_login)

        assert token == "BRAND_NEW"
        assert seen_urls == ["https://kite.example/login"]
        assert session.requested_tokens == ["the_request_token"]

    def test_raises_when_freshly_exchanged_token_fails_post_login_validation(self):
        client = FakeClient(profile_result=_Raise(RuntimeError("still invalid")))
        session = FakeSession(generate_session_result="BAD_NEW_TOKEN")
        manager, *_ = build_manager(client=client, session=session, store=DictTokenStore())

        with pytest.raises(BrokerAuthenticationError):
            manager.ensure_valid_token(on_login_required=lambda url: "req_tok")
