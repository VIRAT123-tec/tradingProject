"""Tests for build_paper_trading_broker -- the wiring that composes paper
mode's real-reads/simulated-writes broker from configs/brokers.yaml.

No network I/O happens building these objects (kiteconnect.KiteConnect's
constructor does not connect), so this is exercised with a temp config root
and fake credential env vars -- no real Kite account needed.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from algo.brokers.exceptions import BrokerAuthenticationError
from algo.brokers.paper_trading_broker import PaperTradingBroker
from algo.services.paper_trading_seams import build_paper_trading_broker

_BROKERS_YAML = """
active_broker: SIMULATION
rate_limits:
  ORDER_MUTATION: {max_calls: 10, per_seconds: 1.0}
  ORDER_READ: {max_calls: 10, per_seconds: 1.0}
  PORTFOLIO_READ: {max_calls: 10, per_seconds: 1.0}
  MARKET_DATA: {max_calls: 10, per_seconds: 1.0}
  INSTRUMENT_LOOKUP: {max_calls: 3, per_seconds: 1.0}
  GENERAL: {max_calls: 10, per_seconds: 1.0}
simulation:
  synchronous: true
  initial_cash: "10000000"
  connection_failure_probability: 0.0
  rejection_probability: 0.0
  partial_fill_probability: 0.0
  ack_latency_seconds: 0.0
  fill_latency_seconds: 0.0
kite:
  api_key_env_var: "KITE_API_KEY"
  api_secret_env_var: "KITE_API_SECRET"
  access_token_env_var: "KITE_ACCESS_TOKEN"
  read_retry_attempts: 3
  read_retry_delay_seconds: 0.0
  quote_batch_size: 200
"""


def _config_root(tmp_path: Path) -> Path:
    root = tmp_path / "configs"
    root.mkdir()
    (root / "brokers.yaml").write_text(_BROKERS_YAML, encoding="utf-8")
    return root


class _FakeAccessTokenStore:
    def __init__(self, token: str | None = "fake-token") -> None:
        self._token = token

    def get_access_token(self):
        return self._token

    def set_access_token(self, access_token):
        self._token = access_token


class TestBuildPaperTradingBroker:
    def test_builds_a_paper_trading_broker_with_no_network_io(self, tmp_path, monkeypatch):
        monkeypatch.setenv("KITE_API_KEY", "test_key")
        monkeypatch.setenv("KITE_API_SECRET", "test_secret")

        broker = build_paper_trading_broker(
            access_token_store=_FakeAccessTokenStore(), config_root=_config_root(tmp_path),
        )
        try:
            assert isinstance(broker, PaperTradingBroker)
            assert broker.broker_name.value == "SIMULATION"
        finally:
            broker.close()

    def test_missing_api_key_raises_a_clear_error(self, tmp_path, monkeypatch):
        # Set (not delete): build_paper_trading_broker() calls load_dotenv()
        # itself, which silently refills an *unset* env var from the real
        # .env -- only an explicitly-set-but-empty value is guaranteed to
        # stay empty (load_dotenv() never overrides an already-present key).
        monkeypatch.setenv("KITE_API_KEY", "")
        monkeypatch.setenv("KITE_API_SECRET", "test_secret")

        with pytest.raises(BrokerAuthenticationError, match="KITE_API_KEY"):
            build_paper_trading_broker(
                access_token_store=_FakeAccessTokenStore(), config_root=_config_root(tmp_path),
            )

    def test_missing_api_secret_raises_a_clear_error(self, tmp_path, monkeypatch):
        monkeypatch.setenv("KITE_API_KEY", "test_key")
        monkeypatch.setenv("KITE_API_SECRET", "")

        with pytest.raises(BrokerAuthenticationError, match="KITE_API_SECRET"):
            build_paper_trading_broker(
                access_token_store=_FakeAccessTokenStore(), config_root=_config_root(tmp_path),
            )
