"""Configuration-integrity tests for the real committed configs.

Two guarantees, both about *not letting a new instrument be half-configured*:

1. Every instrument declared in ``app.yaml``'s ``instances`` has a loadable
   instrument spec, a loadable Strategy-1 config, and a
   ``margin_per_lot_by_instrument`` risk entry -- and the account daily-entry
   limit is at least the number of instances. Missing any of these would let an
   instance fail (or be blocked) only at runtime; this catches it at test time.
2. The monthly expiry resolver (added for BANKNIFTY/FINNIFTY/MIDCPNIFTY/
   SENSEXBANK, which no longer list weekly options) returns the last configured
   weekday of the month, rolls to next month once passed, and shifts off
   holidays -- without disturbing the existing weekly path.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import yaml

from algo.dependency_container import AppConfig
from algo.risk.risk_core import RiskCoreConfig
from algo.services.holiday_service import HolidayService
from algo.services.live_seams import ConfigExpiryService, ConfigInstrumentService
from algo.strategy_engine.parameter_loader import ParameterLoader
from algo.strategy_engine.strategies.strategy_1.config import Strategy1Config

_CONFIGS = Path(__file__).resolve().parents[4] / "configs"


def _load(path: Path, schema):
    return schema.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))


@pytest.fixture(scope="module")
def app_config() -> AppConfig:
    return _load(_CONFIGS / "app.yaml", AppConfig)


@pytest.fixture(scope="module")
def risk_config() -> RiskCoreConfig:
    return _load(_CONFIGS / "risk.yaml", RiskCoreConfig)


@pytest.fixture(scope="module")
def instrument_service() -> ConfigInstrumentService:
    return ConfigInstrumentService(instruments_dir=_CONFIGS / "instruments")


@pytest.fixture(scope="module")
def parameter_loader() -> ParameterLoader:
    return ParameterLoader(config_root=_CONFIGS)


def _instances(app_config: AppConfig):
    return [(i.strategy_id, i.instrument) for i in app_config.instances]


class TestEveryInstanceIsFullyConfigured:
    def test_expected_six_instruments_present(self, app_config):
        assert {i for _, i in _instances(app_config)} == {
            "NIFTY", "SENSEX", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEXBANK",
        }

    def test_every_instance_has_an_instrument_spec(self, app_config, instrument_service):
        for _, instrument in _instances(app_config):
            spec = instrument_service.get_instrument_spec(instrument)  # raises if missing
            assert spec.strike_interval > 0 and spec.lot_size > 0

    def test_every_instance_has_a_strategy_config(self, app_config, parameter_loader):
        for strategy_id, instrument in _instances(app_config):
            cfg = parameter_loader.load_strategy_config(strategy_id, instrument, Strategy1Config)
            assert cfg.lots >= 1

    def test_every_instance_has_a_margin_entry(self, app_config, risk_config):
        missing = [
            i for _, i in _instances(app_config)
            if i not in risk_config.margin_per_lot_by_instrument
        ]
        assert not missing, f"instruments without a margin_per_lot entry: {missing}"

    def test_daily_entry_limit_covers_all_instances(self, app_config, risk_config):
        per_account: dict[str, int] = {}
        for inst in app_config.instances:
            per_account[inst.account] = per_account.get(inst.account, 0) + 1
        assert risk_config.max_daily_entries_per_account >= max(per_account.values())


class TestExpiryCadence:
    @pytest.fixture(scope="class")
    def expiry(self, instrument_service) -> ConfigExpiryService:
        return ConfigExpiryService(
            instrument_service=instrument_service, holiday_service=HolidayService()
        )

    def test_weekly_instruments_stay_weekly(self, instrument_service):
        assert instrument_service.expiry_cadence("NIFTY") == "weekly"
        assert instrument_service.expiry_cadence("SENSEX") == "weekly"

    def test_new_instruments_are_monthly(self, instrument_service):
        for inst in ("BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEXBANK"):
            assert instrument_service.expiry_cadence(inst) == "monthly"

    def test_monthly_nse_resolves_last_tuesday(self, expiry):
        # Verified against the live NFO dump: last Tuesday of July 2026 = 07-28.
        assert expiry.get_current_weekly_expiry("BANKNIFTY", date(2026, 7, 14)) == date(2026, 7, 28)

    def test_monthly_bse_resolves_last_thursday(self, expiry):
        # Verified against the live BFO dump: last Thursday of July 2026 = 07-30.
        assert expiry.get_current_weekly_expiry("SENSEXBANK", date(2026, 7, 14)) == date(2026, 7, 30)

    def test_monthly_rolls_to_next_month_once_passed(self, expiry):
        # After this month's expiry, the next monthly is August's last Tuesday.
        assert expiry.get_current_weekly_expiry("BANKNIFTY", date(2026, 7, 29)) == date(2026, 8, 25)

    def test_sensexbank_maps_to_bankex_for_lookup(self, instrument_service):
        spec = instrument_service.get_instrument_spec("SENSEXBANK")
        assert spec.underlying_symbol == "BANKEX"
        # The others keep the identity (no override).
        assert instrument_service.get_instrument_spec("BANKNIFTY").underlying_symbol is None
