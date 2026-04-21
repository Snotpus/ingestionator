"""Target storage connectors for ingestionator."""

from targets.base import TargetBase
from targets.duckdb import DuckDBStorage
from targets.snowflake import SnowflakeStorage

__all__ = ["TargetBase", "DuckDBStorage", "SnowflakeStorage"]


def setup_targets(factory) -> None:
    """Register all target types with the factory."""
    factory.register("duckdb", lambda cfg: DuckDBStorage(cfg))
    factory.register("snowflake", lambda cfg: SnowflakeStorage(cfg))
