"""Tests for TokenRecord and EnvFileTokenStore.

EnvFileTokenStore is exercised against a real temporary ``.env`` file (no
mocking of ``dotenv`` itself), since its entire job is correctly reading and
writing that file -- a fake would just test the fake.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from algo.brokers.kite.token_store import EnvFileTokenStore, TokenRecord

_IST = timezone(timedelta(hours=5, minutes=30))


# --------------------------------------------------------------------------
# TokenRecord
# --------------------------------------------------------------------------


class TestTokenRecord:
    def test_valid_record(self):
        record = TokenRecord(access_token="TKN", generated_at=datetime(2026, 7, 9, 9, 0, tzinfo=_IST))
        assert record.access_token == "TKN"
        assert record.generated_at.date() == date(2026, 7, 9)

    def test_empty_token_raises(self):
        with pytest.raises(ValueError, match="access_token"):
            TokenRecord(access_token="", generated_at=datetime(2026, 7, 9, 9, 0, tzinfo=_IST))

    def test_naive_datetime_raises(self):
        with pytest.raises(ValueError, match="timezone-aware"):
            TokenRecord(access_token="TKN", generated_at=datetime(2026, 7, 9, 9, 0))


# --------------------------------------------------------------------------
# EnvFileTokenStore
# --------------------------------------------------------------------------


class TestEnvFileTokenStore:
    def test_creates_missing_env_file(self, tmp_path: Path):
        env_path = tmp_path / "nested" / ".env"
        assert not env_path.exists()

        store = EnvFileTokenStore(env_path=env_path)

        assert env_path.exists()
        assert store.env_path == env_path

    def test_get_access_token_missing_returns_none(self, tmp_path: Path):
        store = EnvFileTokenStore(env_path=tmp_path / ".env")
        assert store.get_access_token() is None

    def test_set_and_get_access_token_round_trips(self, tmp_path: Path):
        store = EnvFileTokenStore(env_path=tmp_path / ".env")
        store.set_access_token("ABC123")

        assert store.get_access_token() == "ABC123"
        # Persisted to disk, not just this store's in-memory state.
        assert EnvFileTokenStore(env_path=store.env_path).get_access_token() == "ABC123"

    def test_set_access_token_mirrors_into_os_environ(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("KITE_ACCESS_TOKEN", raising=False)
        store = EnvFileTokenStore(env_path=tmp_path / ".env")
        store.set_access_token("ABC123")

        import os

        assert os.environ["KITE_ACCESS_TOKEN"] == "ABC123"

    def test_get_token_record_missing_returns_none(self, tmp_path: Path):
        store = EnvFileTokenStore(env_path=tmp_path / ".env")
        assert store.get_token_record() is None

    def test_save_and_get_token_record_round_trips(self, tmp_path: Path):
        store = EnvFileTokenStore(env_path=tmp_path / ".env")
        generated_at = datetime(2026, 7, 9, 8, 30, tzinfo=_IST)
        store.save_token(TokenRecord(access_token="FRESH", generated_at=generated_at))

        record = store.get_token_record()
        assert record is not None
        assert record.access_token == "FRESH"
        assert record.generated_at == generated_at

    def test_get_token_record_missing_generated_at_returns_none(self, tmp_path: Path):
        store = EnvFileTokenStore(env_path=tmp_path / ".env")
        store.set_access_token("BARE")  # token only, no generation-time metadata

        assert store.get_token_record() is None

    def test_get_token_record_malformed_timestamp_returns_none(self, tmp_path: Path):
        store = EnvFileTokenStore(env_path=tmp_path / ".env")
        store._write(store._token_var, "TKN")
        store._write(store._generated_at_var, "not-a-timestamp")

        assert store.get_token_record() is None

    def test_get_token_record_naive_timestamp_returns_none(self, tmp_path: Path):
        store = EnvFileTokenStore(env_path=tmp_path / ".env")
        store._write(store._token_var, "TKN")
        store._write(store._generated_at_var, datetime(2026, 7, 9, 8, 0).isoformat())

        assert store.get_token_record() is None

    def test_independent_stores_use_independent_files(self, tmp_path: Path):
        store_a = EnvFileTokenStore(env_path=tmp_path / "a.env")
        store_b = EnvFileTokenStore(env_path=tmp_path / "b.env")

        store_a.set_access_token("TOKEN_A")

        assert store_a.get_access_token() == "TOKEN_A"
        assert store_b.get_access_token() is None

    def test_default_env_path_falls_back_to_dot_env_when_none_is_found(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr("algo.brokers.kite.token_store.find_dotenv", lambda usecwd=True: "")

        store = EnvFileTokenStore()

        assert store.env_path == Path(".env")
        assert store.env_path.exists()

    def test_custom_var_names_are_respected(self, tmp_path: Path):
        store = EnvFileTokenStore(
            env_path=tmp_path / ".env", token_var="MY_TOKEN", generated_at_var="MY_GENERATED_AT",
        )
        generated_at = datetime(2026, 7, 9, 8, 0, tzinfo=_IST)
        store.save_token(TokenRecord(access_token="X", generated_at=generated_at))

        contents = (tmp_path / ".env").read_text()
        assert "MY_TOKEN" in contents
        assert "MY_GENERATED_AT" in contents
        assert "KITE_ACCESS_TOKEN=" not in contents
