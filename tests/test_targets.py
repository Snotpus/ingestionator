"""Tests for targets — DuckDBStorage, SnowflakeStorage, TargetBase."""

import pandas as pd
import pytest

from config import Config
from targets.base import TargetBase
from targets.duckdb import DuckDBStorage
from targets.snowflake import SnowflakeStorage

# --- DuckDBStorage ---

class TestDuckDBStorage:

    @pytest.fixture
    def db_config(self, tmp_path):
        db_path = str(tmp_path / "test.db")
        return Config({
            "target": {
                "type": "duckdb",
                "database": db_path,
                "table": "test_table",
                "mode": "replace",
            },
            "file_to_table": {
                "default": "test_table",
                "users.csv": "users",
            },
        })

    def test_write_and_read_roundtrip(self, db_config, tmp_path):
        db_path = str(tmp_path / "roundtrip.db")
        cfg = Config({
            "target": {
                "type": "duckdb",
                "database": db_path,
                "table": "test_table",
                "mode": "replace",
            },
            "file_to_table": {
                "default": "test_table",
                "users.csv": "users",
            },
        })
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        tgt = DuckDBStorage(cfg, path=db_path)
        tgt.write(df)
        result = tgt.read()
        assert len(result) == 2
        assert list(result.columns) == ["name", "age"]

    def test_write_with_file_mapping(self, tmp_path):
        """Write with filename mapping creates separate table."""
        db_path = str(tmp_path / "mapped.db")
        cfg = Config({
            "target": {
                "type": "duckdb",
                "database": db_path,
                "table": "default_table",
                "mode": "replace",
            },
            "file_to_table": {
                "default": "default_table",
                "users.csv": "users",
            },
        })
        tgt = DuckDBStorage(cfg, path=db_path)
        tgt.write(pd.DataFrame({"x": [1]}), filename="users.csv")
        target = DuckDBStorage(cfg, path=db_path)
        result = target.read(table="users")
        assert len(result) == 1
        assert list(result.columns) == ["x"]

    def test_write_falls_back_to_default(self, tmp_path):
        """Write with unmapped filename falls back to default table."""
        db_path = str(tmp_path / "fallback.db")
        cfg = Config({
            "target": {
                "type": "duckdb",
                "database": db_path,
                "table": "default_table",
                "mode": "replace",
            },
            "file_to_table": {
                "default": "default_table",
                "users.csv": "users",
            },
        })
        tgt = DuckDBStorage(cfg, path=db_path)
        tgt.write(pd.DataFrame({"x": [1]}), filename="unknown.csv")
        result = tgt.read(table="default_table")
        assert len(result) == 1

    def test_write_append_accumulates(self, tmp_path):
        """Multiple writes with append mode accumulate rows."""
        db_path = str(tmp_path / "test2.db")
        cfg = Config({
            "target": {
                "type": "duckdb",
                "database": db_path,
                "table": "test_table",
                "mode": "append",
            },
            "file_to_table": {
                "default": "test_table",
            },
        })
        df1 = pd.DataFrame({"x": [1]})
        tgt = DuckDBStorage(cfg, path=db_path)
        tgt.write(df1)
        df2 = pd.DataFrame({"x": [2, 3]})
        tgt.write(df2)
        result = tgt.read()
        assert len(result) == 3

    def test_mode_replace_drops_table(self, tmp_path):
        """replace mode drops table before creating."""
        temp_db = str(tmp_path / "replace.db")
        cfg = Config({
            "target": {
                "type": "duckdb",
                "database": temp_db,
                "table": "t",
                "mode": "replace",
            },
            "file_to_table": {
                "default": "t",
            },
        })
        tgt = DuckDBStorage(cfg, path=temp_db)
        tgt.write(pd.DataFrame({"a": [1]}))
        assert len(tgt.read()) == 1

    def test_type_name(self):
        cfg = Config({"target": {"type": "duckdb"}})
        tgt = DuckDBStorage(cfg)
        assert tgt.type_name == "duckdb"

    def test_register(self, db_config):
        from factories import Factory
        f = Factory(db_config, register_defaults=False)
        tgt = DuckDBStorage(db_config)
        tgt.register(f)
        assert "duckdb" in f._registry

    def test_resolve_table_with_directory_name(self, tmp_path):
        """Parent directory name is used as table when filename doesn't match."""
        db_path = str(tmp_path / "dir_table.db")
        cfg = Config({
            "target": {
                "type": "duckdb",
                "database": db_path,
                "table": "default_table",
                "mode": "replace",
            },
            "file_to_table": {
                "default": "default_table",
                "customers": "customers_table",
            },
        })
        tgt = DuckDBStorage(cfg, path=db_path)
        assert tgt._resolve_table("customers/20240112/datafile.csv") == "customers_table"
        assert tgt._resolve_table("customers/20240212/datafile1.csv") == "customers_table"

    def test_write_uses_directory_as_table_name(self, tmp_path):
        """Files in customer directory write to customers_table."""
        db_path = str(tmp_path / "dir_writes.db")
        cfg = Config({
            "target": {
                "type": "duckdb",
                "database": db_path,
                "table": "default_table",
                "mode": "replace",
            },
            "file_to_table": {
                "default": "default_table",
                "customers": "customers_table",
            },
        })
        tgt = DuckDBStorage(cfg, path=db_path)
        tgt.write(pd.DataFrame({"id": [1, 2]}), filename="customers/20240112/datafile.csv")
        result = tgt.read(table="customers_table")
        assert len(result) == 2

    def test_filename_match_takes_priority_over_directory(self, tmp_path):
        """Exact filename match wins over directory name match."""
        db_path = str(tmp_path / "priority.db")
        cfg = Config({
            "target": {"type": "duckdb", "database": db_path, "table": "t", "mode": "replace"},
            "file_to_table": {
                "default": "default_table",
                "datafile.csv": "file_override",
                "customers": "dir_override",
            },
        })
        tgt = DuckDBStorage(cfg, path=db_path)
        assert tgt._resolve_table("customers/datafile.csv") == "file_override"


# --- SnowflakeStorage ---

class TestSnowflakeStorage:

    @pytest.fixture
    def sf_config(self):
        return Config({
            "target": {
                "type": "snowflake",
                "table": "ingested",
                "mode": "replace",
                "snowflake": {
                    "account": "testacct",
                    "database": "TEST_DB",
                    "schema": "PUBLIC",
                    "warehouse": "ETL_WH",
                    "role": "ADMIN",
                    "user_secret": "USER",
                    "password_secret": "PASS",
                },
            },
            "file_to_table": {
                "default": "ingested",
                "users.csv": "users",
            },
        })

    def test_type_name(self):
        cfg = Config({"target": {"type": "snowflake"}})
        tgt = SnowflakeStorage(cfg)
        assert tgt.type_name == "snowflake"

    def test_connection_params_from_config(self, sf_config):
        tgt = SnowflakeStorage(sf_config)
        assert tgt._account == "testacct"
        assert tgt._database == "TEST_DB"
        assert tgt._warehouse == "ETL_WH"

    def test_resolve_table_with_mapping(self, sf_config):
        tgt = SnowflakeStorage(sf_config)
        assert tgt._resolve_table("users.csv") == "users"
        assert tgt._resolve_table("other.csv") == "ingested"

    def test_resolve_table_without_mapping(self):
        cfg = Config({"target": {"type": "snowflake", "table": "mytbl"}})
        tgt = SnowflakeStorage(cfg)
        assert tgt._resolve_table("any.csv") == "mytbl"

    def test_register(self, sf_config):
        from factories import Factory
        f = Factory(sf_config, register_defaults=False)
        tgt = SnowflakeStorage(sf_config)
        tgt.register(f)
        assert "snowflake" in f._registry

    def test_write_with_file_mapping(self):
        """SnowflakeStorage write resolves table from filename."""
        cfg = Config({
            "target": {
                "type": "snowflake",
                "table": "default",
                "mode": "replace",
                "snowflake": {
                    "account": "testacct",
                    "database": "TEST_DB",
                    "schema": "PUBLIC",
                    "warehouse": "ETL_WH",
                    "role": "ADMIN",
                    "user_secret": "USER",
                    "password_secret": "PASS",
                },
            },
            "file_to_table": {
                "default": "default",
                "users.csv": "users_table",
            },
        })
        tgt = SnowflakeStorage(cfg)
        assert tgt._resolve_table("users.csv") == "users_table"
        assert tgt._resolve_table("other.csv") == "default"

    def test_resolve_table_with_directory_name(self):
        """Parent directory name is used as table when filename doesn't match."""
        cfg = Config({
            "target": {
                "type": "snowflake",
                "table": "ingested",
                "mode": "replace",
                "snowflake": {
                    "account": "testacct",
                    "database": "TEST_DB",
                    "schema": "PUBLIC",
                    "warehouse": "ETL_WH",
                    "role": "ADMIN",
                    "user_secret": "USER",
                    "password_secret": "PASS",
                },
            },
            "file_to_table": {
                "default": "ingested",
                "customers": "customers_table",
            },
        })
        tgt = SnowflakeStorage(cfg)
        assert tgt._resolve_table("customers/20240112/datafile.csv") == "customers_table"
        assert tgt._resolve_table("customers/20240212/datafile1.csv") == "customers_table"

    def test_resolve_table_with_directory_name_and_filename_override(self):
        """Exact filename match wins over directory name match."""
        cfg = Config({
            "target": {"type": "snowflake", "table": "default", "mode": "replace", "snowflake":
                        {"account": "a", "database": "DB", "user_secret": "U", "password_secret": "P"}},
            "file_to_table": {
                "default": "default",
                "datafile.csv": "file_override",
                "customers": "dir_override",
            },
        })
        tgt = SnowflakeStorage(cfg)
        assert tgt._resolve_table("customers/datafile.csv") == "file_override"


# --- TargetBase ---

class TestTargetBase:

    def test_abstract_cannot_instantiate(self):
        with pytest.raises(TypeError):
            TargetBase()
