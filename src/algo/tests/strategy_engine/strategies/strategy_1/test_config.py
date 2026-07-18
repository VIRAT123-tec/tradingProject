"""Unit tests for the Strategy-1 configuration schema.

Covers per-field validation, the two cross-field sanity checks, immutability,
and loading the two real project YAML files
(configs/strategies/strategy_1/{nifty,sensex}.yaml) end-to-end through the
same ParameterLoader path the strategy engine uses at startup.
"""

from __future__ import annotations

from datetime import time
from decimal import Decimal

import pytest
from pydantic import ValidationError

from algo.common.enums import ProductType
from algo.strategy_engine.parameter_loader import ParameterLoader
from algo.strategy_engine.strategies.strategy_1.config import RetrySettings, Strategy1Config


def _valid_retry(**overrides) -> RetrySettings:
    base = dict(
        order_timeout_seconds=None,
        fill_confirmation_attempts=20,
        fill_confirmation_delay_seconds=0.25,
        close_retry_attempts=3,
        close_retry_delay_seconds=0.5,
    )
    base.update(overrides)
    return RetrySettings(**base)


def _valid_config(**overrides) -> Strategy1Config:
    base = dict(
        entry_time=time(9, 20),
        hard_cutoff_time=time(15, 15),
        target_pct=Decimal("0.10"),
        sl_pct=Decimal("0.10"),
        lots=1,
        product_type=ProductType.INTRADAY,
        skip_on_expiry_day=False,
        monitoring_interval_seconds=5.0,
        polling_interval_seconds=2.0,
        retry=_valid_retry(),
    )
    base.update(overrides)
    return Strategy1Config(**base)


# --------------------------------------------------------------------------
# Happy path
# --------------------------------------------------------------------------


class TestValidConfig:
    def test_constructs_with_all_fields(self):
        config = _valid_config()
        assert config.entry_time == time(9, 20)
        assert config.hard_cutoff_time == time(15, 15)
        assert config.target_pct == Decimal("0.10")
        assert config.sl_pct == Decimal("0.10")
        assert config.lots == 1
        assert config.product_type is ProductType.INTRADAY
        assert config.skip_on_expiry_day is False
        assert config.monitoring_interval_seconds == 5.0
        assert config.polling_interval_seconds == 2.0
        assert isinstance(config.retry, RetrySettings)

    def test_is_immutable(self):
        config = _valid_config()
        with pytest.raises(ValidationError):
            config.lots = 2  # type: ignore[misc]

    def test_retry_settings_is_immutable(self):
        retry = _valid_retry()
        with pytest.raises(ValidationError):
            retry.fill_confirmation_attempts = 5  # type: ignore[misc]

    def test_order_timeout_none_is_a_valid_explicit_value(self):
        config = _valid_config(retry=_valid_retry(order_timeout_seconds=None))
        assert config.retry.order_timeout_seconds is None

    def test_order_timeout_can_be_set(self):
        config = _valid_config(retry=_valid_retry(order_timeout_seconds=3.5))
        assert config.retry.order_timeout_seconds == 3.5


# --------------------------------------------------------------------------
# Missing required fields -- no hidden defaults
# --------------------------------------------------------------------------


class TestMissingFieldsFailFast:
    @pytest.mark.parametrize(
        "field",
        [
            "entry_time",
            "hard_cutoff_time",
            "target_pct",
            "sl_pct",
            "lots",
            "product_type",
            "skip_on_expiry_day",
            "monitoring_interval_seconds",
            "polling_interval_seconds",
            "retry",
        ],
    )
    def test_missing_top_level_field_raises(self, field):
        kwargs = dict(
            entry_time=time(9, 20),
            hard_cutoff_time=time(15, 15),
            target_pct=Decimal("0.10"),
            sl_pct=Decimal("0.10"),
            lots=1,
            product_type=ProductType.INTRADAY,
            skip_on_expiry_day=False,
            monitoring_interval_seconds=5.0,
            polling_interval_seconds=2.0,
            retry=_valid_retry(),
        )
        del kwargs[field]
        with pytest.raises(ValidationError):
            Strategy1Config(**kwargs)

    @pytest.mark.parametrize(
        "field",
        [
            "order_timeout_seconds",
            "fill_confirmation_attempts",
            "fill_confirmation_delay_seconds",
            "close_retry_attempts",
            "close_retry_delay_seconds",
        ],
    )
    def test_missing_retry_field_raises(self, field):
        kwargs = dict(
            order_timeout_seconds=None,
            fill_confirmation_attempts=20,
            fill_confirmation_delay_seconds=0.25,
            close_retry_attempts=3,
            close_retry_delay_seconds=0.5,
        )
        del kwargs[field]
        with pytest.raises(ValidationError):
            RetrySettings(**kwargs)


# --------------------------------------------------------------------------
# Per-field bounds
# --------------------------------------------------------------------------


class TestFieldBounds:
    @pytest.mark.parametrize("bad_pct", [Decimal("0"), Decimal("1"), Decimal("1.5"), Decimal("-0.1")])
    def test_target_pct_out_of_range_raises(self, bad_pct):
        with pytest.raises(ValidationError):
            _valid_config(target_pct=bad_pct)

    @pytest.mark.parametrize("bad_pct", [Decimal("0"), Decimal("1"), Decimal("2"), Decimal("-0.5")])
    def test_sl_pct_out_of_range_raises(self, bad_pct):
        with pytest.raises(ValidationError):
            _valid_config(sl_pct=bad_pct)

    @pytest.mark.parametrize("bad_lots", [0, -1])
    def test_lots_must_be_positive(self, bad_lots):
        with pytest.raises(ValidationError):
            _valid_config(lots=bad_lots)

    @pytest.mark.parametrize("bad_value", [0, -1])
    def test_monitoring_interval_must_be_positive(self, bad_value):
        with pytest.raises(ValidationError):
            _valid_config(monitoring_interval_seconds=bad_value, polling_interval_seconds=bad_value)

    @pytest.mark.parametrize("bad_value", [0, -1])
    def test_polling_interval_must_be_positive(self, bad_value):
        with pytest.raises(ValidationError):
            _valid_config(polling_interval_seconds=bad_value)

    def test_invalid_product_type_string_raises(self):
        with pytest.raises(ValidationError):
            _valid_config(product_type="NOT_A_REAL_PRODUCT")

    @pytest.mark.parametrize("bad_value", [0, -1])
    def test_fill_confirmation_attempts_must_be_positive(self, bad_value):
        with pytest.raises(ValidationError):
            _valid_config(retry=_valid_retry(fill_confirmation_attempts=bad_value))

    @pytest.mark.parametrize("bad_value", [0, -0.5])
    def test_fill_confirmation_delay_must_be_positive(self, bad_value):
        with pytest.raises(ValidationError):
            _valid_config(retry=_valid_retry(fill_confirmation_delay_seconds=bad_value))

    @pytest.mark.parametrize("bad_value", [0, -1])
    def test_close_retry_attempts_must_be_positive(self, bad_value):
        with pytest.raises(ValidationError):
            _valid_config(retry=_valid_retry(close_retry_attempts=bad_value))

    @pytest.mark.parametrize("bad_value", [0, -0.5])
    def test_close_retry_delay_must_be_positive(self, bad_value):
        with pytest.raises(ValidationError):
            _valid_config(retry=_valid_retry(close_retry_delay_seconds=bad_value))

    def test_order_timeout_zero_or_negative_raises(self):
        with pytest.raises(ValidationError):
            _valid_config(retry=_valid_retry(order_timeout_seconds=0))
        with pytest.raises(ValidationError):
            _valid_config(retry=_valid_retry(order_timeout_seconds=-1))


# --------------------------------------------------------------------------
# Cross-field validation
# --------------------------------------------------------------------------


class TestCrossFieldValidation:
    def test_entry_time_must_be_before_cutoff(self):
        with pytest.raises(ValidationError, match="entry_time"):
            _valid_config(entry_time=time(15, 15), hard_cutoff_time=time(9, 20))

    def test_entry_time_equal_to_cutoff_raises(self):
        with pytest.raises(ValidationError, match="entry_time"):
            _valid_config(entry_time=time(9, 20), hard_cutoff_time=time(9, 20))

    def test_entry_before_cutoff_is_valid(self):
        config = _valid_config(entry_time=time(9, 20), hard_cutoff_time=time(15, 15))
        assert config.entry_time < config.hard_cutoff_time

    def test_polling_looser_than_monitoring_raises(self):
        with pytest.raises(ValidationError, match="polling_interval_seconds"):
            _valid_config(monitoring_interval_seconds=2.0, polling_interval_seconds=5.0)

    def test_polling_equal_to_monitoring_is_valid(self):
        config = _valid_config(monitoring_interval_seconds=3.0, polling_interval_seconds=3.0)
        assert config.polling_interval_seconds == config.monitoring_interval_seconds

    def test_polling_tighter_than_monitoring_is_valid(self):
        config = _valid_config(monitoring_interval_seconds=5.0, polling_interval_seconds=1.0)
        assert config.polling_interval_seconds < config.monitoring_interval_seconds


# --------------------------------------------------------------------------
# End-to-end load through the real project YAML files
# --------------------------------------------------------------------------


class TestRealConfigFiles:
    @pytest.mark.parametrize("instrument", ["nifty", "sensex"])
    def test_loads_and_validates_real_project_yaml(self, instrument):
        loader = ParameterLoader()  # default config_root: "configs/" (project root)
        config = loader.load_strategy_config("strategy_1", instrument, Strategy1Config)

        assert isinstance(config, Strategy1Config)
        assert config.entry_time < config.hard_cutoff_time
        assert config.polling_interval_seconds <= config.monitoring_interval_seconds
        assert Decimal("0") < config.target_pct < Decimal("1")
        assert Decimal("0") < config.sl_pct < Decimal("1")

    def test_nifty_and_sensex_configs_are_independently_loaded(self):
        loader = ParameterLoader()
        nifty = loader.load_strategy_config("strategy_1", "nifty", Strategy1Config)
        sensex = loader.load_strategy_config("strategy_1", "sensex", Strategy1Config)
        # Same schema, independently parsed instances -- not the same object,
        # each read from its own instrument's file.
        assert nifty is not sensex
        assert nifty.retry is not sensex.retry
