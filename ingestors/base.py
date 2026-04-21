"""Abstract base class and registry for data ingestors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from config import Config


class IngestorBase(ABC):
    """Abstract base class defining the ingestor interface."""

    type_name: str = ""

    def __init__(self, config: Config):
        self.config = config

    @abstractmethod
    def ingest(self, data: bytes) -> pd.DataFrame:
        """Parse raw file bytes into a DataFrame."""
        ...

    def register(self, factory: Any) -> None:
        """Register this ingestor type with the factory registry."""
        if self.type_name:
            factory.register(self.type_name, lambda cfg: self.__class__(cfg))
