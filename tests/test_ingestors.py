"""Tests for ingestors — CsvIngestor, ParquetIngestor, IngestorBase."""

import tempfile

import pandas as pd
import pytest

from config import Config
from ingestors.base import IngestorBase
from ingestors.csv import CsvIngestor
from ingestors.parquet import ParquetIngestor

# --- CsvIngestor ---

class TestCsvIngestor:

    @pytest.fixture
    def csv_config(self):
        return Config({
            "ingestor": {
                "type": "csv",
                "csv": {"delimiter": ",", "encoding": "utf-8", "header": 0, "skip_blank_lines": True},
            }
        })

    def test_ingest_parses_csv(self, csv_config):
        data = b"name,age\nAlice,30\nBob,25"
        ing = CsvIngestor(csv_config)
        df = ing.ingest(data)
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 2
        assert list(df.columns) == ["name", "age"]
        assert df.iloc[0]["name"] == "Alice"

    def test_ingest_uses_config_delimiter(self, csv_config):
        csv_config._raw["ingestor"]["csv"]["delimiter"] = ";"
        ing = CsvIngestor(csv_config)
        df = ing.ingest(b"name;age\nAlice;30")
        assert list(df.columns) == ["name", "age"]

    def test_ingest_empty_data_raises(self, csv_config):
        ing = CsvIngestor(csv_config)
        with pytest.raises(pd.errors.EmptyDataError):
            ing.ingest(b"")

    def test_type_name(self):
        cfg = Config({"ingestor": {"type": "csv", "csv": {}}})
        ing = CsvIngestor(cfg)
        assert ing.type_name == "csv"

    def test_register(self, csv_config):
        from factories import Factory
        f = Factory(csv_config, register_defaults=False)
        ing = CsvIngestor(csv_config)
        ing.register(f)
        assert "csv" in f._registry


# --- ParquetIngestor ---

class TestParquetIngestor:

    @pytest.fixture
    def pq_config(self):
        return Config({
            "ingestor": {
                "type": "parquet",
                "parquet": {"engine": "pyarrow"},
            }
        })

    def test_ingest_parses_parquet(self, pq_config):
        """Write a parquet file to disk and read it back."""
        df_expected = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 25]})
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            df_expected.to_parquet(f, engine="pyarrow")
            f.flush()
            with open(f.name, "rb") as rf:
                data = rf.read()

        ing = ParquetIngestor(pq_config)
        ing._options = {"engine": "pyarrow"}
        result = ing.ingest(data)
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert list(result.columns) == ["name", "age"]

    def test_type_name(self):
        assert ParquetIngestor.type_name == "parquet"

    def test_register(self, pq_config):
        from factories import Factory
        f = Factory(pq_config, register_defaults=False)
        ing = ParquetIngestor(pq_config)
        ing.register(f)
        assert "parquet" in f._registry

    def test_has_ingest_method(self):
        """ParquetIngestor must implement ingest() (inherited from IngestorBase)."""
        ing = ParquetIngestor.__new__(ParquetIngestor)
        assert hasattr(ing, "ingest")
        assert hasattr(ing, "process")


# --- IngestorBase ---

class TestIngestorBase:

    def test_abstract_cannot_instantiate(self):
        with pytest.raises(TypeError):
            IngestorBase()
