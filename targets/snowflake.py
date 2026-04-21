"""Snowflake target storage connector."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

from targets.base import TargetBase

_TABLE_NAME_RE = __import__("re").compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_table_name(name: str) -> str:
    if not _TABLE_NAME_RE.match(name):
        raise ValueError(f"Invalid table name: {name!r}. Must match ^[a-zA-Z_][a-zA-Z0-9_]*$")
    return name

if TYPE_CHECKING:
    from config import Config


class SnowflakeStorage(TargetBase):
    """Writes to and reads from Snowflake."""

    type_name = "snowflake"

    def __init__(self, config: Config,
                 account: str | None = None,
                 database: str | None = None,
                 schema: str | None = None,
                 warehouse: str | None = None,
                 role: str | None = None):
        super().__init__(config)
        self._account = account or config.get("target.snowflake.account")
        self._database = database or config.get("target.snowflake.database")
        self._schema = schema or config.get("target.snowflake.schema", "PUBLIC")
        self._warehouse = warehouse or config.get("target.snowflake.warehouse")
        self._role = role or config.get("target.snowflake.role")
        self._table = _validate_table_name(config.get("target.table", "ingested_data"))
        self._mode = config.get("target.mode", "replace")
        self._file_to_table = config.get("file_to_table", {})
        self._conn = None

    def _get_connection(self):
        """Lazy initialize Snowflake connection."""
        if self._conn is None:
            import snowflake.connector as snowconn
            user_secret = self.config.get("target.snowflake.user_secret", "USER")
            password_secret = self.config.get("target.snowflake.password_secret", "PASSWORD")

            # Resolve secret references
            from secret_manager import SecretManager
            sm = SecretManager()
            user = sm.resolve(user_secret)
            password = sm.resolve(password_secret)

            self._conn = snowconn.connect(
                account=self._account,
                user=user,
                password=password,
                database=self._database,
                schema=self._schema,
                warehouse=self._warehouse,
                role=self._role,
            )
        return self._conn

    def _resolve_table(self, filename: str | None = None) -> str:
        """Resolve table name from filename using file_to_table mapping."""
        if filename and self._file_to_table:
            default_table = self._file_to_table.get("default", "ingested_data")
            mapping = {k: v for k, v in self._file_to_table.items() if k != "default"}
            resolved = mapping.get(filename, default_table)
            return _validate_table_name(resolved)
        return self._table

    def write(self, df: pd.DataFrame, filename: str | None = None) -> None:
        """Write a DataFrame to a Snowflake table using batch inserts."""
        conn = self._get_connection()
        cursor = conn.cursor()
        table = self._resolve_table(filename)

        if self._mode == "replace":
            cursor.execute(f"DROP TABLE IF EXISTS {table}")

        # Create table if it doesn't exist
        if df.empty:
            return

        columns = [f"`{col}` VARCHAR" for col in df.columns]
        create_sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(columns)})"
        cursor.execute(create_sql)

        # Build insert statement
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_sql = f"INSERT INTO {table} VALUES ({placeholders})"

        # Batch insert
        batch_size = 1000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i + batch_size]
            values = [tuple(row) for _, row in batch.iterrows()]
            cursor.executemany(insert_sql, values)

        conn.commit()
        cursor.close()

    def read(self) -> pd.DataFrame:
        """Read all data from the target table."""
        conn = self._get_connection()
        query = f"SELECT * FROM {self._table}"
        result = conn.cursor().execute(query).fetch_pandas_all()
        return result
