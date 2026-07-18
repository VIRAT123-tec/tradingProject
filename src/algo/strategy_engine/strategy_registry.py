"""StrategyRegistry: the ``strategy_id`` -> strategy-class lookup.

This is the seam that lets ``instance_factory`` build a runnable instance from
a configuration string ("strategy_1") without importing any concrete strategy
class. New strategies register themselves; the factory only ever sees the
abstract ``Strategy`` type.

A single process-wide ``default_registry`` plus the ``@register_strategy``
decorator is the normal registration path. The class is still explicitly
instantiable so tests (and any future multi-tenant wiring) can build isolated
registries instead of mutating global state.
"""

from __future__ import annotations

from algo.strategy_engine.strategy_base import Strategy


class StrategyRegistryError(Exception):
    """Base class for registry errors."""


class StrategyAlreadyRegisteredError(StrategyRegistryError):
    """Raised when registering a ``strategy_id`` that is already taken.

    Deliberately a hard error rather than a silent overwrite: two strategies
    claiming the same id is a wiring bug that must surface at import time, not
    a last-writer-wins surprise discovered at 09:20.
    """


class StrategyNotRegisteredError(StrategyRegistryError, KeyError):
    """Raised when looking up a ``strategy_id`` that was never registered.

    Subclasses ``KeyError`` so existing ``except KeyError`` call sites still
    catch it, while remaining distinguishable as a registry-specific failure.
    """


class StrategyRegistry:
    """A mapping of ``strategy_id`` to concrete ``Strategy`` subclass."""

    def __init__(self) -> None:
        self._strategies: dict[str, type[Strategy]] = {}

    def register(self, strategy_id: str, strategy_cls: type[Strategy]) -> None:
        """Register ``strategy_cls`` under ``strategy_id``.

        Raises ``ValueError`` if the id is empty or the class is not a
        ``Strategy`` subclass, and ``StrategyAlreadyRegisteredError`` if the
        id is already taken (registering the *same* class under the same id
        again is tolerated as an idempotent no-op, which makes duplicate
        imports harmless).
        """
        if not strategy_id:
            raise ValueError("strategy_id must be a non-empty string")
        if not isinstance(strategy_cls, type) or not issubclass(strategy_cls, Strategy):
            raise ValueError(
                f"strategy_cls for {strategy_id!r} must be a Strategy subclass, "
                f"got {strategy_cls!r}"
            )
        existing = self._strategies.get(strategy_id)
        if existing is not None and existing is not strategy_cls:
            raise StrategyAlreadyRegisteredError(
                f"strategy_id {strategy_id!r} is already registered to "
                f"{existing.__name__}; refusing to overwrite with "
                f"{strategy_cls.__name__}"
            )
        self._strategies[strategy_id] = strategy_cls

    def get(self, strategy_id: str) -> type[Strategy]:
        """Return the class registered under ``strategy_id``.

        Raises ``StrategyNotRegisteredError`` if nothing is registered under
        that id.
        """
        try:
            return self._strategies[strategy_id]
        except KeyError:
            raise StrategyNotRegisteredError(
                f"no strategy registered under id {strategy_id!r}; "
                f"known ids: {sorted(self._strategies)}"
            ) from None

    def is_registered(self, strategy_id: str) -> bool:
        """Whether any class is registered under ``strategy_id``."""
        return strategy_id in self._strategies

    def registered_ids(self) -> frozenset[str]:
        """The set of all registered strategy ids (snapshot)."""
        return frozenset(self._strategies)

    def unregister(self, strategy_id: str) -> None:
        """Remove a registration. Primarily for test isolation; a no-op if
        the id is not present."""
        self._strategies.pop(strategy_id, None)


default_registry = StrategyRegistry()
"""The process-wide registry the ``@register_strategy`` decorator writes to and
``instance_factory`` reads from by default."""


def register_strategy(strategy_id: str):
    """Class decorator registering a ``Strategy`` subclass on the
    ``default_registry`` under ``strategy_id``.

    Usage (in a concrete strategy module, later)::

        @register_strategy("strategy_1")
        class Strategy1(Strategy):
            ...

    Returns the class unchanged so it remains usable normally.
    """

    def decorator(strategy_cls: type[Strategy]) -> type[Strategy]:
        default_registry.register(strategy_id, strategy_cls)
        return strategy_cls

    return decorator
