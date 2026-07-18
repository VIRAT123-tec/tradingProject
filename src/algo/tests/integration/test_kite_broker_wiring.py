"""Kite broker, wired through a real DependencyContainer, with the
``kiteconnect`` SDK mocked at the process boundary -- the one external
dependency this platform cannot exercise for real in a test (no network, no
real Kite credentials). Everything *this side* of that boundary is real:
``DependencyContainer._build_kite_broker``'s construction, ``KiteSession``'s
activation, ``RateLimitedBroker``'s wrapping, ``KiteBroker`` itself, and
``mapper.py``'s request/response/exception translation.

What this file does NOT re-test (already covered by
``tests/brokers/kite/test_kite_broker.py``'s own unit suite, Task 21): every
mapper.py translation case, every retry-vs-never-retry branch for every
BrokerError subtype, rate-limit throttling details. This file checks that the
container actually builds a *working* KiteBroker from ``brokers.yaml`` +
environment variables + an injected ``AccessTokenStore`` -- the one thing no
existing test exercises, since Task 21's suite builds ``KiteBroker`` directly
and Task 24's suite only tests the *rejection* path (no token store).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from kiteconnect import exceptions as kite_exc

from algo.brokers.broker_base import PlaceOrderRequest
from algo.brokers.exceptions import OrderRejectedError
from algo.brokers.rate_limiter import RateLimitedBroker
from algo.common.enums import BrokerName, Exchange, OrderType, ProductType, TransactionType
from algo.tests.integration.conftest import FakeExpiryService, FakeInstrumentService, FakeSpotPriceProvider, FakeTickStream, make_clock


class FakeAccessTokenStore:
    def __init__(self, token: str | None = "the-access-token") -> None:
        self._token = token

    def get_access_token(self) -> str | None:
        return self._token

    def set_access_token(self, access_token: str) -> None:
        self._token = access_token


def _kite_config_root(tmp_path: Path) -> Path:
    """A temp config root selecting the Kite broker -- everything else
    copied unmodified from the real, committed configs."""
    import shutil

    real_root = Path("configs")
    root = tmp_path / "configs"
    root.mkdir()
    for name in ("app.yaml", "accounts.yaml", "risk.yaml", "market_data.yaml"):
        shutil.copy(real_root / name, root / name)
    shutil.copytree(real_root / "strategies", root / "strategies")
    (root / "brokers.yaml").write_text(
        (real_root / "brokers.yaml").read_text(encoding="utf-8").replace(
            "active_broker: SIMULATION", "active_broker: KITE"
        ),
        encoding="utf-8",
    )
    return root


@pytest.fixture(autouse=True)
def _database_url_env(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite:///unused")


@pytest.fixture(autouse=True)
def _kite_credentials_env(monkeypatch):
    # Names match configs/brokers.yaml's kite.api_key_env_var / api_secret_env_var.
    monkeypatch.setenv("KITE_API_KEY", "test-api-key")
    monkeypatch.setenv("KITE_API_SECRET", "test-api-secret")


def _build_kite_container(tmp_path: Path, mock_client: MagicMock):
    from algo.dependency_container import DependencyContainer

    root = _kite_config_root(tmp_path)
    with patch("kiteconnect.KiteConnect", return_value=mock_client):
        return DependencyContainer(
            config_root=root,
            instrument_service=FakeInstrumentService(),
            expiry_service=FakeExpiryService(),
            spot_price_provider=FakeSpotPriceProvider(),
            tick_stream=FakeTickStream(),
            time_provider=make_clock(),
            access_token_store=FakeAccessTokenStore(),
        )


class TestContainerBuildsWorkingKiteBroker:
    def test_broker_is_rate_limited_kite_broker(self, tmp_path):
        from algo.brokers.kite.kite_broker import KiteBroker

        mock_client = MagicMock()
        container = _build_kite_container(tmp_path, mock_client)

        assert isinstance(container.broker, RateLimitedBroker)
        assert isinstance(container.broker._inner, KiteBroker)  # noqa: SLF001

    def test_kite_client_built_with_bounded_request_timeout(self, tmp_path):
        # M2: the KiteConnect client must be constructed with a socket timeout,
        # so every call -- including mutating place/modify/cancel, which the SDK
        # has no per-call timeout for -- is bounded rather than unbounded.
        mock_client = MagicMock()
        root = _kite_config_root(tmp_path)
        with patch("kiteconnect.KiteConnect", return_value=mock_client) as mock_cls:
            from algo.dependency_container import DependencyContainer

            DependencyContainer(
                config_root=root, instrument_service=FakeInstrumentService(),
                expiry_service=FakeExpiryService(), spot_price_provider=FakeSpotPriceProvider(),
                tick_stream=FakeTickStream(), time_provider=make_clock(),
                access_token_store=FakeAccessTokenStore(),
            )
        _args, kwargs = mock_cls.call_args
        assert kwargs.get("timeout") == 7.0  # from the committed brokers.yaml

    def test_authenticate_activates_session_and_checks_profile(self, tmp_path):
        mock_client = MagicMock()
        mock_client.profile.return_value = {"user_id": "AB1234"}
        container = _build_kite_container(tmp_path, mock_client)

        container.broker.authenticate()

        mock_client.set_access_token.assert_called_once_with("the-access-token")
        mock_client.profile.assert_called_once()

    def test_authenticate_without_a_token_raises_cleanly(self, tmp_path):
        from algo.brokers.exceptions import BrokerAuthenticationError

        mock_client = MagicMock()
        root = _kite_config_root(tmp_path)
        with patch("kiteconnect.KiteConnect", return_value=mock_client):
            from algo.dependency_container import DependencyContainer

            container = DependencyContainer(
                config_root=root,
                instrument_service=FakeInstrumentService(),
                expiry_service=FakeExpiryService(),
                spot_price_provider=FakeSpotPriceProvider(),
                tick_stream=FakeTickStream(),
                time_provider=make_clock(),
                access_token_store=FakeAccessTokenStore(token=None),
            )

        with pytest.raises(BrokerAuthenticationError):
            container.broker.authenticate()


class TestPlaceOrderThroughRealMapper:
    def test_successful_placement_returns_the_broker_order_id(self, tmp_path):
        mock_client = MagicMock()
        mock_client.place_order.return_value = "251231000000001"
        container = _build_kite_container(tmp_path, mock_client)

        request = PlaceOrderRequest(
            exchange=Exchange.NFO, tradingsymbol="NIFTY26JUL0925000CE",
            transaction_type=TransactionType.SELL, quantity=75,
            product=ProductType.INTRADAY, order_type=OrderType.MARKET,
            tag="entry-tag-1",
        )
        result = container.broker.place_order(request)

        assert result.broker_order_id == "251231000000001"
        call_kwargs = mock_client.place_order.call_args.kwargs
        assert call_kwargs["tradingsymbol"] == "NIFTY26JUL0925000CE"
        assert call_kwargs["quantity"] == 75
        assert call_kwargs["tag"] == "entry-tag-1"

    def test_kite_order_exception_translates_to_order_rejected(self, tmp_path):
        mock_client = MagicMock()
        mock_client.place_order.side_effect = kite_exc.OrderException("margin insufficient")
        container = _build_kite_container(tmp_path, mock_client)

        request = PlaceOrderRequest(
            exchange=Exchange.NFO, tradingsymbol="NIFTY26JUL0925000CE",
            transaction_type=TransactionType.SELL, quantity=75,
            product=ProductType.INTRADAY, order_type=OrderType.MARKET,
            tag="entry-tag-2",
        )

        with pytest.raises(OrderRejectedError, match="margin insufficient"):
            container.broker.place_order(request)


class TestBrokerSelectionIntegration:
    def test_container_selects_kite_when_configured(self, tmp_path):
        mock_client = MagicMock()
        container = _build_kite_container(tmp_path, mock_client)

        assert container.brokers_config.active_broker is BrokerName.KITE
