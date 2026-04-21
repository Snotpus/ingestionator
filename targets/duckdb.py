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

    def __init__(self, config: Config, path: str | None = None):
        super().__init__(config)
        self._db_path = path or config.get("target.database", "./output/ingested.db")
        self._mode = config.get("target.mode", "replace")
        self._file_to_table = config.get("file_to_table", {})
        self._table = _validate_table_name(config.get("target.table", "ingested_data"))

    def _resolve_table(self, filename: str | None = None) -> str:
        """Resolve table name from filename using file_to_table mapping."""
        if filename and self._file_to_table:
            default_table = self._file_to_table.get("default", "ingested_data")
            mapping = {k: v for k, v in self._file_to_table.items() if k != "default"}
            resolved = mapping.get(filename, default_table)
            return _validate_table_name(resolved)
        return self._table

    def write(self, df: pd.DataFrame, filename: str | None = None) -> None:
        """Write a DataFrame to a DuckDB table."""
        import os
        table = self._resolve_table(filename)
        self._last_write_table = table
        os.makedirs(os.path.dirname(self._db_path) or ".", exist_ok=True)
        conn = duckdb.connect(self._db_path)
        if self._mode == "replace":
            conn.execute(f"DROP TABLE IF EXISTS {table}")
            conn.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
        else:
            conn.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM df LIMIT 0")
            conn.execute(f"INSERT INTO {table} SELECT * FROM df")
        conn.close()

    def read(self, table: str | None = None) -> pd.DataFrame:
        """Read all data from the target table."""
        if table is None:
            table = self._table
        conn = duckdb.connect(self._db_path)
        result = conn.execute(f"SELECT * FROM {table}").fetchdf()
        conn.close()
        return result
