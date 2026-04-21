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
            }
        })

    def test_write_and_read_roundtrip(self, db_config, tmp_path):
        db_path = str(tmp_path / "roundtrip.db")
        cfg = Config({
            "target": {
                "type": "duckdb",
                "database": db_path,
                "table": "test_table",
                "mode": "replace",
            }
        })
        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        tgt = DuckDBStorage(cfg, path=db_path)
        tgt.write(df)
        result = tgt.read()
        assert len(result) == 2
        assert list(result.columns) == ["name", "age"]

    def test_write_append_accumulates(self, tmp_path):
        """Multiple writes with append mode accumulate rows."""
        db_path = str(tmp_path / "test2.db")
        cfg = Config({
            "target": {
                "type": "duckdb",
                "database": db_path,
                "table": "test_table",
                "mode": "append",
            }
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
            }
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

    def test_register(self, sf_config):
        from factories import Factory
        f = Factory(sf_config, register_defaults=False)
        tgt = SnowflakeStorage(sf_config)
        tgt.register(f)
        assert "snowflake" in f._registry


# --- TargetBase ---

class TestTargetBase:

    def test_abstract_cannot_instantiate(self):
        with pytest.raises(TypeError):
            TargetBase()
