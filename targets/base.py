"""Abstract base class and registry for target storage connectors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from config import Config


class TargetBase(ABC):
    """Abstract base class defining the target storage interface."""

    type_name: str = ""

    def __init__(self, config: Config):
        self.config = config

    @abstractmethod
    def write(self, df: pd.DataFrame) -> None:
        """Write a DataFrame to the target storage."""
        ...

    @abstractmethod
    def read(self) -> pd.DataFrame:
        """Read data from the target storage and return as DataFrame."""
        ...

    def register(self, factory: Any) -> None:
        """Register this target type with the factory registry."""
        if self.type_name:
            factory.register(self.type_name, lambda cfg: self.__class__(cfg))
