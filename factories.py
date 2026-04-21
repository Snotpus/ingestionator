"""Factory registry for creating source, ingestor, and target connectors."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, TypeVar

_T = TypeVar("_T")


class Factory:
    """Registry that creates storage connectors by type string."""

    def __init__(self, config: Any = None, register_defaults: bool = True):
        self.config = config
        self._registry: dict[str, Callable] = {}
        if register_defaults:
            self._register_defaults()

    def register(self, type_name: str, factory_fn: Callable) -> None:
        """Register a factory function for the given type name."""
        if type_name in self._registry:
            raise ValueError(f"Factory type already registered: {type_name!r}")
        self._registry[type_name] = factory_fn

    def create(self, type_name: str) -> Any:
        """Create an instance of the given type. Passes config to the factory function."""
        if type_name not in self._registry:
            raise LookupError(
                f"Unregistered factory type: {type_name!r}. "
                f"Available: {sorted(self._registry)}"
            )
        return self._registry[type_name](self.config)

    def _register_defaults(self) -> None:
        """Register all connector types from their respective packages.
        Actual connectors are auto-imported; setup_* functions in each package
        can also be called externally to register additional or custom types."""
        from ingestors import setup_ingestors
        from sources import setup_sources
        from targets import setup_targets
        setup_sources(self)
        setup_ingestors(self)
        setup_targets(self)


def get_factory(config: Any = None) -> Factory:
    """Module-level convenience to create a factory."""
    return Factory(config)
