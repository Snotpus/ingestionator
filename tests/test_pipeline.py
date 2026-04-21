"""Tests for pipeline.py — Pipeline class and retry logic."""

import os
import tempfile
from unittest import mock

import pytest

from config import Config
from error_handling import IngestionError, IngestorError, SourceError, TargetError, retry_with_backoff
from factories import Factory
from pipeline import Pipeline

# --- Pipeline ---

class TestPipeline:

    @pytest.fixture
    def tmpdir(self):
        with tempfile.TemporaryDirectory() as td:
            yield td

    @pytest.fixture
    def csv_config(self, tmpdir):
        db_path = os.path.join(tmpdir, "test.db")
        return Config({
            "source": {"type": "local", "path": tmpdir, "file_pattern": "*.csv"},
            "ingestor": {"type": "csv", "csv": {"delimiter": ",", "header": 0}},
            "target": {"type": "duckdb", "database": db_path, "table": "data", "mode": "replace"},
            "error_handling": {"retry_attempts": 1, "backoff_factor": 2, "max_delay": 1},
        })

    def test_full_pipeline_run(self, tmpdir, csv_config):
        csv_path = os.path.join(tmpdir, "data.csv")
        with open(csv_path, "w") as f:
            f.write("a,b\n1,x\n2,y\n3,z\n")
        pipeline = Pipeline(csv_config, Factory(csv_config))
        rows = pipeline.run()
        assert rows == 3

    def test_run_file_single(self, tmpdir, csv_config):
        csv_path = os.path.join(tmpdir, "single.csv")
        with open(csv_path, "w") as f:
            f.write("col\nonly\n")
        pipeline = Pipeline(csv_config, Factory(csv_config))
        rows = pipeline.run_file(csv_path)
        assert rows == 1

    def test_total_rows_accumulates(self, tmpdir, csv_config):
        for i in range(3):
            path = os.path.join(tmpdir, f"file{i}.csv")
            with open(path, "w") as f:
                f.write(f"x\n{i}\n")
        pipeline = Pipeline(csv_config, Factory(csv_config))
        pipeline.run()
        assert pipeline.total_rows == 3

    def test_empty_directory_returns_zero(self, tmpdir, csv_config):
        pipeline = Pipeline(csv_config, Factory(csv_config))
        assert pipeline.run() == 0

    def test_creates_correct_connectors(self, csv_config):
        pipeline = Pipeline(csv_config, Factory(csv_config))
        assert pipeline._source.__class__.__name__ == "LocalSource"
        assert pipeline._ingestor.__class__.__name__ == "CsvIngestor"
        assert pipeline._target.__class__.__name__ == "DuckDBStorage"

    def test_source_error_propagates(self, csv_config):
        pipeline = Pipeline(csv_config, Factory(csv_config))
        with mock.patch.object(pipeline._source, "list_files", side_effect=SourceError("disk fault")):
            with pytest.raises(IngestionError, match="disk fault"):
                pipeline.run()

    def test_ingestor_error_propagates(self, tmpdir, csv_config):
        csv_path = os.path.join(tmpdir, "test.csv")
        with open(csv_path, "w") as f:
            f.write("x\n1\n")
        pipeline = Pipeline(csv_config, Factory(csv_config))
        with mock.patch.object(pipeline._ingestor, "ingest", side_effect=IngestorError("parse fail")):
            with pytest.raises(IngestionError, match="parse fail"):
                pipeline.run_file(csv_path)

    def test_target_error_propagates(self, tmpdir, csv_config):
        csv_path = os.path.join(tmpdir, "test.csv")
        with open(csv_path, "w") as f:
            f.write("x\n1\n")
        pipeline = Pipeline(csv_config, Factory(csv_config))
        with mock.patch.object(pipeline._target, "write", side_effect=TargetError("write fail")):
            with pytest.raises(IngestionError, match="write fail"):
                pipeline.run_file(csv_path)


# --- retry_with_backoff ---

class TestRetryWithBackoff:

    def test_succeeds_first_attempt(self):
        call_count = 0

        @retry_with_backoff
        def always_ok():
            nonlocal call_count
            call_count += 1
            return "ok"

        assert always_ok() == "ok"
        assert call_count == 1

    def test_retries_on_failure_then_succeeds(self):
        call_count = 0

        @retry_with_backoff
        def sometimes_ok():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise IngestionError(f"attempt {call_count}")
            return "ok"

        assert sometimes_ok() == "ok"
        assert call_count == 3

    def test_exhausts_attempts_and_raises(self):
        call_count = 0

        @retry_with_backoff
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise IngestionError("always fails")

        with pytest.raises(IngestionError, match="always fails"):
            always_fails()
        assert call_count == 3  # default attempts=3

    def test_source_error_triggers_retry(self):
        call_count = 0

        @retry_with_backoff
        def fails_then_ok():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise SourceError("source fail")
            return "ok"

        assert fails_then_ok() == "ok"
        assert call_count == 2

    def test_target_error_triggers_retry(self):
        call_count = 0

        @retry_with_backoff
        def fails_then_ok():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise TargetError("target fail")
            return "ok"

        assert fails_then_ok() == "ok"
        assert call_count == 2

    def test_default_attempts_is_three(self):
        call_count = 0

        @retry_with_backoff
        def always_fails():
            nonlocal call_count
            call_count += 1
            raise IngestionError("fail")

        with pytest.raises(IngestionError):
            always_fails()
        assert call_count == 3  # default is 3 attempts
