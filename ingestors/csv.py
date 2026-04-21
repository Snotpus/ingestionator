"""CSV file ingestor."""

from __future__ import annotations

import io
from typing import TYPE_CHECKING

import pandas as pd

from ingestors.base import IngestorBase

if TYPE_CHECKING:
    from config import Config


class CsvIngestor(IngestorBase):
    """Parse CSV bytes into a DataFrame."""

    type_name = "csv"

    def __init__(self, config: Config):
        super().__init__(config)
        self._options = config.get_section("ingestor.csv")

    def ingest(self, data: bytes) -> pd.DataFrame:
        return pd.read_csv(io.BytesIO(data), **self._options)
