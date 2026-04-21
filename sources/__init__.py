"""Data source connectors for ingestionator."""

from sources.base import SourceBase
from sources.local import LocalSource
from sources.s3 import S3Source

__all__ = ["SourceBase", "LocalSource", "S3Source"]


def setup_sources(factory) -> None:
    """Register all source types with the factory."""
    factory.register("local", lambda cfg: LocalSource(cfg))
    factory.register("s3", lambda cfg: S3Source(cfg))
