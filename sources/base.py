"""Abstract base class and registry for data source connectors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    from config import Config


class SourceBase(ABC):
    """Abstract base class defining the source interface."""

    type_name: str = ""

    def __init__(self, config: Config):
        self.config = config

    @abstractmethod
    def list_files(self) -> list[str]:
        """Return a list of file paths/keys available from this source."""
        ...

    @abstractmethod
    def read_file(self, path: str) -> bytes:
        """Read raw bytes from the given path/key."""
        ...

    def read_file_df(self, path: str) -> pd.DataFrame:
        """Read file and return raw bytes — subclasses should override to return a DataFrame."""
        return pd.read_json(self.read_file(path))

    def register(self, factory: Any) -> None:
        """Register this source type with the factory registry."""
        if self.type_name:
            factory.register(self.type_name, lambda cfg: self.__class__(cfg))
