"""Tests for scripts/generate_token.py.

``_extract_request_token`` is pure parsing and tested directly. ``main`` is
tested by monkeypatching ``_build_token_manager`` to return a fake
TokenManager, so the whole CLI flow (skip-if-valid, --force, error handling)
is exercised with no real browser, network, or Kite credentials -- the fake
stands in exactly where TokenManager itself is already unit-tested
separately in test_token_manager.py.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pytest

import scripts.generate_token as generate_token


# --------------------------------------------------------------------------
# _extract_request_token
# --------------------------------------------------------------------------


class TestExtractRequestToken:
    def test_bare_token_passes_through(self):
        assert generate_token._extract_request_token("abc123token") == "abc123token"

    def test_strips_surrounding_whitespace(self):
        assert generate_token._extract_request_token("  abc123token  ") == "abc123token"

    def test_full_redirect_url(self):
        url = "https://127.0.0.1/?request_token=xyz789&action=login&status=success"
        assert generate_token._extract_request_token(url) == "xyz789"

    def test_bare_query_string(self):
        assert generate_token._extract_request_token("?request_token=qwe&status=success") == "qwe"

    def test_url_with_other_params_first(self):
        url = "https://example.com/callback?status=success&request_token=lastparam"
        assert generate_token._extract_request_token(url) == "lastparam"


# --------------------------------------------------------------------------
# main()
# --------------------------------------------------------------------------


class _Raise:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc


@dataclass
class FakeTokenManager:
    check_existing_result: object = None
    ensure_valid_token_result: object = "NEW_TOKEN"
    check_existing_calls: int = 0
    ensure_valid_token_calls: int = 0

    def check_existing(self):
        self.check_existing_calls += 1
        if isinstance(self.check_existing_result, _Raise):
            raise self.check_existing_result.exc
        return self.check_existing_result

    def ensure_valid_token(self, *, on_login_required=None):
        self.ensure_valid_token_calls += 1
        if isinstance(self.ensure_valid_token_result, _Raise):
            raise self.ensure_valid_token_result.exc
        return self.ensure_valid_token_result


class TestMain:
    def test_skips_login_when_a_valid_token_already_exists(self, monkeypatch: pytest.MonkeyPatch):
        fake = FakeTokenManager(check_existing_result="EXISTING_TOKEN")
        monkeypatch.setattr(generate_token, "_build_token_manager", lambda: fake)

        assert generate_token.main([]) == 0
        assert fake.check_existing_calls == 1
        assert fake.ensure_valid_token_calls == 0

    def test_force_skips_the_existing_token_check(self, monkeypatch: pytest.MonkeyPatch):
        fake = FakeTokenManager(check_existing_result="EXISTING_TOKEN", ensure_valid_token_result="FORCED_NEW")
        monkeypatch.setattr(generate_token, "_build_token_manager", lambda: fake)

        assert generate_token.main(["--force"]) == 0
        assert fake.check_existing_calls == 0
        assert fake.ensure_valid_token_calls == 1

    def test_runs_login_flow_when_no_valid_token_exists(self, monkeypatch: pytest.MonkeyPatch):
        fake = FakeTokenManager(check_existing_result=None, ensure_valid_token_result="MINTED")
        monkeypatch.setattr(generate_token, "_build_token_manager", lambda: fake)

        assert generate_token.main([]) == 0
        assert fake.check_existing_calls == 1
        assert fake.ensure_valid_token_calls == 1

    def test_returns_nonzero_when_token_manager_cannot_be_built(self, monkeypatch: pytest.MonkeyPatch):
        def boom():
            raise RuntimeError("KITE_API_KEY and KITE_API_SECRET must both be set in .env")

        monkeypatch.setattr(generate_token, "_build_token_manager", boom)

        assert generate_token.main([]) == 1

    def test_returns_nonzero_when_login_flow_fails(self, monkeypatch: pytest.MonkeyPatch):
        from algo.brokers.exceptions import BrokerAuthenticationError

        fake = FakeTokenManager(
            check_existing_result=None,
            ensure_valid_token_result=_Raise(BrokerAuthenticationError("bad request token")),
        )
        monkeypatch.setattr(generate_token, "_build_token_manager", lambda: fake)

        assert generate_token.main([]) == 1
