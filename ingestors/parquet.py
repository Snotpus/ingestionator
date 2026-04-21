"""Parquet file ingestor."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from ingestors.base import IngestorBase

if TYPE_CHECKING:
    from config import Config


class ParquetIngestor(IngestorBase):
    """Parse Parquet bytes into a DataFrame."""

    type_name = "parquet"

    def __init__(self, config: Config):
        super().__init__(config)
        self._options = config.get_section("ingestor.parquet")

    def ingest(self, raw_bytes: bytes) -> pd.DataFrame:
        """Parse parquet bytes into a DataFrame."""
        return self.process(raw_bytes)

    def process(self, raw_bytes: bytes) -> pd.DataFrame:
        import io
        return pd.read_parquet(io.BytesIO(raw_bytes), **{k: v for k, v in self._options.items() if v is not None})
