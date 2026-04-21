"""Tests for config.py — Config class, load, validate, get."""

import tempfile

import pytest
import yaml

from config import Config, ConfigurationError, load, load_config

# --- Config construction ---

class TestConfigInit:

    def test_empty_config_stores_raw(self):
        cfg = Config({})
        # __init__ applies defaults for non-source/target keys
        assert "ingestor" in cfg._raw
        assert "error_handling" in cfg._raw
        assert "logging" in cfg._raw

    def test_to_dict_returns_copy(self):
        cfg = Config({"source": {"type": "local"}})
        d = cfg.to_dict()
        assert d is not cfg._raw
        assert d["source"]["type"] == "local"

    def test_repr(self):
        cfg = Config({"source": {"type": "local"}, "target": {"type": "duckdb"}})
        assert repr(cfg) == "Config(source=local, target=duckdb)"


# --- get() ---

class TestConfigGet:

    def test_dot_notation_access(self):
        cfg = Config({"a": {"b": {"c": 42}}})
        assert cfg.get("a.b.c") == 42

    def test_flat_access(self):
        cfg = Config({"x": 1})
        assert cfg.get("x") == 1

    def test_default_on_missing_key(self):
        cfg = Config({})
        assert cfg.get("missing", "default") == "default"

    def test_default_none_on_missing_key(self):
        cfg = Config({})
        assert cfg.get("missing") is None

    def test_get_section(self):
        cfg = Config({"a": {"b": 1, "c": 2}})
        assert cfg.get_section("a") == {"b": 1, "c": 2}

    def test_get_section_missing_returns_empty(self):
        cfg = Config({})
        assert cfg.get_section("nonexistent") == {}


# --- validate() ---

class TestValidate:

    def test_valid_config_passes(self):
        cfg = Config({"source": {"type": "local", "path": "/tmp"}, "target": {"type": "duckdb"}})
        cfg.validate()  # no exception

    def test_invalid_source_type_raises(self):
        cfg = Config({"source": {"type": "ftp", "path": "/tmp"}, "target": {"type": "duckdb"}})
        with pytest.raises(ConfigurationError, match="Invalid source type"):
            cfg.validate()

    def test_invalid_target_type_raises(self):
        cfg = Config({"source": {"type": "local", "path": "/tmp"}, "target": {"type": "bigquery"}})
        with pytest.raises(ConfigurationError, match="Invalid target type"):
            cfg.validate()

    def test_empty_source_path_raises(self):
        cfg = Config({"source": {"type": "local", "path": ""}, "target": {"type": "duckdb"}})
        with pytest.raises(ConfigurationError, match="source.path is required"):
            cfg.validate()

    def test_validate_applies_defaults(self):
        cfg = Config({"source": {"type": "local", "path": "/tmp"}})
        cfg.validate()
        assert cfg.get("target.type") == "duckdb"
        assert cfg.get("target.database") == "./output/ingested.db"
        assert cfg.get("ingestor.csv.delimiter") == ","

    def test_validate_all_same_as_validate(self):
        cfg = Config({"source": {"type": "local", "path": "/tmp"}, "target": {"type": "duckdb"}})
        cfg.validate_all()  # should not raise

    def test_validate_fills_missing_target_section(self):
        cfg = Config({"source": {"type": "local", "path": "/tmp"}})
        cfg.validate()
        assert cfg.get("target.type") == "duckdb"

    def test_source_type_defaults_after_validate(self):
        cfg = Config({"source": {}, "target": {}})
        cfg.validate()
        assert cfg.get("source.type") == "local"
        assert cfg.get("source.path") == "./test_data"


# --- load() ---

class TestLoad:

    def test_load_returns_config(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"source": {"type": "local", "path": "/tmp"}, "target": {"type": "duckdb"}}, f)
            f.flush()
            cfg = load(f.name)
        assert isinstance(cfg, Config)

    def test_load_nonexistent_file_raises(self):
        with pytest.raises(ConfigurationError, match="Config file not found"):
            load("/no/such/file.yaml")

    def test_load_invalid_yaml_raises(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("{bad yaml:::")
            f.flush()
            with pytest.raises(ConfigurationError, match="Invalid YAML"):
                load(f.name)

    def test_load_non_mapping_raises(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("- item1\n- item2\n")
            f.flush()
            with pytest.raises(ConfigurationError, match="Config must be a YAML mapping"):
                load(f.name)


# --- load_config() ---

class TestLoadConfig:

    def test_load_config_loads_and_validates(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"source": {"type": "local", "path": "/tmp"}, "target": {"type": "duckdb"}}, f)
            f.flush()
            cfg = load_config(f.name)
        cfg.validate()  # should not raise

    def test_load_config_invalid_raises(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"source": {"type": "gopher", "path": ""}, "target": {"type": "duckdb"}}, f)
            f.flush()
            with pytest.raises(ConfigurationError):
                load_config(f.name)
