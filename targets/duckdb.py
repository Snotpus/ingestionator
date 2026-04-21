"""DuckDB target storage connector."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import duckdb
import pandas as pd

from targets.base import TargetBase

if TYPE_CHECKING:
    from config import Config

_TABLE_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_table_name(name: str) -> str:
    """Validate a table name to prevent SQL injection."""
    if not _TABLE_NAME_RE.match(name):
        raise ValueError(f"Invalid table name: {name!r}. Must match ^[a-zA-Z_][a-zA-Z0-9_]*$")
    return name


class DuckDBStorage(TargetBase):
    """Writes to and reads from a DuckDB database."""

    type_name = "duckdb"

    def __init__(self, config: Config, path: str | None = None, table: str | None = None):
        super().__init__(config)
        self._db_path = path or config.get("target.database", "./output/ingested.db")
        self._table = _validate_table_name(table or config.get("target.table", "ingested_data"))
        self._mode = config.get("target.mode", "replace")

    def write(self, df: pd.DataFrame) -> None:
        """Write a DataFrame to a DuckDB table."""
        import os

        os.makedirs(os.path.dirname(self._db_path) or ".", exist_ok=True)
        conn = duckdb.connect(self._db_path)
        if self._mode == "replace":
            conn.execute(f"DROP TABLE IF EXISTS {self._table}")
            conn.execute(f"CREATE TABLE {self._table} AS SELECT * FROM df")
        else:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {self._table} AS SELECT * FROM df LIMIT 0")
            conn.execute(f"INSERT INTO {self._table} SELECT * FROM df")
        conn.close()

    def read(self) -> pd.DataFrame:
        """Read all data from the target table."""
        conn = duckdb.connect(self._db_path)
        result = conn.execute(f"SELECT * FROM {self._table}").fetchdf()
        conn.close()
        return result
