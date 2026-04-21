"""Integration tests for ingestionator — end-to-end pipeline wiring."""

import os
import tempfile
from unittest import mock

import pytest

from config import Config, ConfigurationError, load
from error_handling import IngestionError, IngestorError, SourceError, TargetError
from factories import Factory
from ingestors import setup_ingestors
from pipeline import Pipeline
from sources import setup_sources
from targets import setup_targets


@pytest.fixture
def tmpdir():
    """Temporary directory for integration test artifacts."""
    with tempfile.TemporaryDirectory() as td:
        yield td


@pytest.fixture
def config_path(tmpdir):
    """Write a minimal config.yaml and return its path."""
    import yaml
    cfg = {
        "pipeline": {"name": "test", "version": "0.1"},
        "source": {"type": "local", "path": tmpdir, "file_pattern": "*.csv"},
        "ingestor": {
            "type": "csv",
            "csv": {"delimiter": ",", "encoding": "utf-8", "header": 0, "skip_blank_lines": True},
            "parquet": {"engine": "pyarrow"},
        },
        "target": {
            "type": "duckdb",
            "database": os.path.join(tmpdir, "output", "test.db"),
            "table": "test_data",
            "mode": "replace",
        },
        "secrets": {"aws": {"enabled": False}, "env_prefix": "TEST"},
        "error_handling": {"retry_attempts": 1, "backoff_factor": 2, "max_delay": 1},
        "logging": {"level": "CRITICAL"},
    }
    path = os.path.join(tmpdir, "config.yaml")
    with open(path, "w") as f:
        yaml.dump(cfg, f)
    return path


@pytest.fixture
def csv_file(tmpdir):
    """Write a CSV file and return its path."""
    path = os.path.join(tmpdir, "sample.csv")
    with open(path, "w") as f:
        f.write("name,age,city\nAlice,30,NYC\nBob,25,LA\n")
    return path


# ----------------------
# 1. Full pipeline run: CSV from local filesystem -> DuckDB
# ----------------------

class TestPipelineE2E:

    def test_full_pipeline_run(self, config_path, csv_file):
        """Data flows: CSV file -> DataFrame -> DuckDB, end-to-end."""
        cfg = load(config_path)
        cfg.validate()
        factory = Factory(cfg)
        # factory auto-registers all types via _register_defaults
        pipeline = Pipeline(cfg, factory)
        rows = pipeline.run()
        assert rows == 2

        # Verify data persisted in DuckDB
        target = factory.create("duckdb")
        result = target.read()
        assert len(result) == 2
        assert list(result.columns) == ["name", "age", "city"]

    def test_run_single_file(self, config_path, csv_file):
        """Ingesting a single file returns correct row count."""
        cfg = load(config_path)
        cfg.validate()
        factory = Factory(cfg)
        pipeline = Pipeline(cfg, factory)
        rows = pipeline.run_file(csv_file)
        assert rows == 2

    def test_pipeline_creates_correct_connectors(self, config_path):
        """Factory wiring creates correct connector types."""
        cfg = load(config_path)
        cfg.validate()
        factory = Factory(cfg)

        src = factory.create("local")
        assert src.__class__.__name__ == "LocalSource"

        ing = factory.create("csv")
        assert ing.__class__.__name__ == "CsvIngestor"

        tgt = factory.create("duckdb")
        assert tgt.__class__.__name__ == "DuckDBStorage"


# ----------------------
# 2. Config schema validation
# ----------------------

class TestConfigValidation:

    def test_validate_fills_defaults_for_missing_sections(self):
        """validate() fills in missing required sections so config is usable."""
        cfg = Config({"source": {"type": "local", "path": "."}})
        cfg.validate()
        assert cfg.get("target.type") == "duckdb"
        assert cfg.get("source.type") == "local"

    def test_validate_catches_invalid_source_type(self):
        cfg = Config({"source": {"type": "gopher", "path": "."}, "target": {"type": "duckdb"}})
        with pytest.raises(ConfigurationError, match="Invalid source type"):
            cfg.validate()

    def test_validate_catches_invalid_target_type(self):
        cfg = Config({"source": {"type": "local", "path": "."}, "target": {"type": "azure"}})
        with pytest.raises(ConfigurationError, match="Invalid target type"):
            cfg.validate()

    def test_validate_catches_missing_source_path(self):
        # Empty path is still a missing path after defaults won't override explicit value
        cfg = Config({"source": {"type": "local", "path": ""}, "target": {"type": "duckdb"}})
        with pytest.raises(ConfigurationError, match="source.path is required"):
            cfg.validate()

    def test_validate_applies_defaults_for_missing_sections(self):
        """After validate() fills in defaults, .get() returns correct values."""
        cfg = Config({"source": {"type": "local", "path": "."}})
        cfg.validate()
        assert cfg.get("target.type") == "duckdb"
        assert cfg.get("target.database") == "./output/ingested.db"

    def test_validate_applies_source_defaults(self):
        """After validate() fills in source defaults, source works."""
        cfg = Config({"source": {"type": "local"}, "target": {"type": "duckdb"}})
        cfg.validate()
        assert cfg.get("source.path") == "./test_data"

    def test_load_returns_config(self, config_path):
        cfg = load(config_path)
        assert cfg.get("source.type") == "local"
        assert cfg.get("target.type") == "duckdb"

    def test_load_config_loads_and_validates(self, config_path):
        from config import load_config
        cfg = load_config(config_path)
        assert cfg.get("source.type") == "local"

    def test_load_nonexistent_file(self):
        with pytest.raises(ConfigurationError, match="Config file not found"):
            load("/nonexistent/config.yaml")


# ----------------------
# 3. Error handling propagation
# ----------------------

class TestErrorHandling:

    def test_source_error_propagates(self, config_path, tmpdir):
        """Pipeline raises IngestionError (via subclass) on source failure."""
        cfg = load(config_path)
        cfg.validate()
        factory = Factory(cfg)
        pipeline = Pipeline(cfg, factory)

        with mock.patch.object(pipeline._source, "list_files",
                               side_effect=SourceError("disk read error")), \
             pytest.raises(IngestionError, match="disk read error"):
            pipeline.run()

    def test_ingestor_error_propagates(self, csv_file, config_path):
        """Pipeline raises IngestionError on ingestor failure."""
        cfg = load(config_path)
        cfg.validate()
        factory = Factory(cfg)
        pipeline = Pipeline(cfg, factory)

        with mock.patch.object(pipeline._ingestor, "ingest",
                               side_effect=IngestorError("bad data")), \
             pytest.raises(IngestionError, match="bad data"):
            pipeline.run_file(csv_file)

    def test_target_error_propagates(self, csv_file, config_path):
        """Pipeline raises IngestionError on target failure."""
        cfg = load(config_path)
        cfg.validate()
        factory = Factory(cfg)
        pipeline = Pipeline(cfg, factory)

        with mock.patch.object(pipeline._target, "write",
                               side_effect=TargetError("write failed")), \
             pytest.raises(IngestionError, match="write failed"):
            pipeline.run_file(csv_file)

    def test_main_returns_exit_code_on_failure(self, config_path, tmpdir):
        # Write a valid config but with a source file that doesn't exist as a file
        import yaml

        from main import main
        bad_cfg = {
            "pipeline": {"name": "test", "version": "0.1"},
            "source": {"type": "local", "path": "/nonexistent/path/xyz", "file_pattern": "*.csv"},
            "ingestor": {"type": "csv", "csv": {"delimiter": ",", "encoding": "utf-8", "header": 0}},
            "target": {"type": "duckdb", "database": os.path.join(tmpdir, "out.db"), "table": "t", "mode": "replace"},
            "secrets": {"aws": {"enabled": False}},
            "error_handling": {"retry_attempts": 1, "backoff_factor": 2},
            "logging": {"level": "CRITICAL"},
        }
        bad_path = os.path.join(tmpdir, "bad_config.yaml")
        with open(bad_path, "w") as f:
            yaml.dump(bad_cfg, f)
        # local source with nonexistent dir -> list_files returns [] -> pipeline runs fine (0 rows)
        # Use a nonexistent file override to trigger pipeline failure
        code = main(["--config", bad_path, "--file", "/no/such/file.csv"])
        assert code == 1

    def test_main_returns_exit_code_zero_on_success(self, config_path, csv_file):
        from main import main
        code = main(["--config", config_path, "--file", csv_file])
        assert code == 0

    def test_main_returns_exit_code_2_on_bad_config(self):
        from main import main
        code = main(["--config", "/no/such/file.yaml"])
        assert code == 2


# ----------------------
# 4. Factory registration completeness
# ----------------------

class TestFactoryWiring:

    def test_all_source_types_registered(self, tmpdir):
        cfg = Config({"source": {}, "ingestor": {}, "target": {}})
        f = Factory(cfg, register_defaults=False)
        setup_sources(f)
        assert "local" in f._registry
        assert "s3" in f._registry

    def test_all_ingestor_types_registered(self, tmpdir):
        cfg = Config({"source": {}, "ingestor": {}, "target": {}})
        f = Factory(cfg, register_defaults=False)
        setup_ingestors(f)
        assert "csv" in f._registry
        assert "parquet" in f._registry

    def test_all_target_types_registered(self, tmpdir):
        cfg = Config({"source": {}, "ingestor": {}, "target": {}})
        f = Factory(cfg, register_defaults=False)
        setup_targets(f)
        assert "duckdb" in f._registry
        assert "snowflake" in f._registry

    def test_duplicate_registration_raises(self, tmpdir):
        cfg = Config({"source": {}, "ingestor": {}, "target": {}})
        f = Factory(cfg, register_defaults=False)
        f.register("local", lambda cfg: None)
        with pytest.raises(ValueError, match="already registered"):
            f.register("local", lambda cfg: None)

    def test_unregistered_type_raises(self, tmpdir):
        cfg = Config({"source": {}, "ingestor": {}, "target": {}})
        f = Factory(cfg, register_defaults=False)
        with pytest.raises(LookupError, match="Unregistered factory type"):
            f.create("phantom")

    def test_auto_registered_connectors_work(self, tmpdir):
        """Factory with defaults creates working connectors."""
        cfg = Config({
            "source": {"type": "local", "path": tmpdir},
            "ingestor": {"type": "csv", "csv": {}},
            "target": {"type": "duckdb", "database": "/dev/null", "table": "x", "mode": "replace"},
        })
        f = Factory(cfg)
        src = f.create("local")
        assert src.__class__.__name__ == "LocalSource"
