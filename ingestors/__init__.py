"""Data ingestors for ingestionator."""

from ingestors.base import IngestorBase
from ingestors.csv import CsvIngestor
from ingestors.parquet import ParquetIngestor

__all__ = ["IngestorBase", "CsvIngestor", "ParquetIngestor"]


def setup_ingestors(factory) -> None:
    """Register all ingestor types with the factory."""
    factory.register("csv", lambda cfg: CsvIngestor(cfg))
    factory.register("parquet", lambda cfg: ParquetIngestor(cfg))
