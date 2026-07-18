"""ParameterLoader: load and validate per-strategy, per-instrument YAML config
into a typed Pydantic model, failing loud at startup rather than at market open.

Strategy-agnostic by construction: it never knows which fields a config has, only
how to read a YAML file and validate it into a schema the *caller* supplies (the
schema a strategy declares via ``Strategy.config_schema()``). A malformed or
missing config raises here, at process start, not at 09:20 when an order is due.

Path convention (matching how ``database.py`` resolves ``configs/``):
``{config_root}/strategies/{strategy_id}/{instrument}.yaml`` with the instrument
name lower-cased -- e.g. ``configs/strategies/strategy_1/nifty.yaml``.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, TypeVar

import yaml
from pydantic import BaseModel

ModelT = TypeVar("ModelT", bound=BaseModel)


def _default_config_root() -> Path:
    """Resolve the ``configs/`` root the same way ``database.py`` does:
    ``configs`` relative to the working directory, overridable via the
    ``CONFIG_DIR`` environment variable."""
    return Path(os.environ.get("CONFIG_DIR", "configs"))


class ParameterLoader:
    """Loads YAML config files and validates them into caller-supplied
    Pydantic models."""

    def __init__(self, config_root: Path | None = None) -> None:
        """``config_root`` defaults to the shared ``configs/`` resolution;
        tests pass an explicit path to a fixture directory."""
        self._config_root = config_root if config_root is not None else _default_config_root()

    @property
    def config_root(self) -> Path:
        """The root directory configs are resolved under."""
        return self._config_root

    def strategy_config_path(self, strategy_id: str, instrument: str) -> Path:
        """Resolve the config path for one (strategy, instrument) pair by
        convention. Does not check existence -- see ``load``."""
        return self._config_root / "strategies" / strategy_id / f"{instrument.lower()}.yaml"

    def load(self, path: Path, schema: type[ModelT]) -> ModelT:
        """Read the YAML at ``path`` and validate it into ``schema``.

        Raises ``FileNotFoundError`` if the file is absent, ``ValueError`` if
        its top level is not a mapping, and ``pydantic.ValidationError`` if
        any field is missing or invalid -- all deliberately loud and fatal at
        startup. An empty file is treated as an empty mapping, so a schema
        whose fields are all optional still loads (and one with required
        fields still fails, as it should).
        """
        if not path.exists():
            raise FileNotFoundError(
                f"config file not found: {path} (expected a YAML file for "
                f"schema {schema.__name__})"
            )

        with path.open("r", encoding="utf-8") as f:
            raw: Any = yaml.safe_load(f)

        if raw is None:
            raw = {}
        if not isinstance(raw, dict):
            raise ValueError(
                f"config file {path} must contain a YAML mapping at the top "
                f"level, got {type(raw).__name__}"
            )

        return schema.model_validate(raw)

    def load_strategy_config(
        self, strategy_id: str, instrument: str, schema: type[ModelT]
    ) -> ModelT:
        """Convenience: resolve the conventional path for a (strategy,
        instrument) pair and validate it into ``schema``."""
        return self.load(self.strategy_config_path(strategy_id, instrument), schema)
