"""Tests for sources — LocalSource, S3Source, SourceBase."""

import os
import tempfile
from unittest import mock

import pandas as pd
import pytest

from config import Config
from sources.base import SourceBase
from sources.local import LocalSource
from sources.s3 import S3Source

# --- LocalSource ---

class TestLocalSource:

    @pytest.fixture
    def tmpdir(self):
        with tempfile.TemporaryDirectory() as td:
            yield td

    @pytest.fixture
    def config(self, tmpdir):
        return Config({"source": {"type": "local", "path": tmpdir, "file_pattern": "*.txt"}})

    def test_list_files_finds_matching(self, tmpdir, config):
        path = os.path.join(tmpdir, "hello.txt")
        with open(path, "w") as f:
            f.write("data")
        src = LocalSource(config)
        files = src.list_files()
        assert len(files) == 1
        assert files[0] == path

    def test_list_files_no_match(self, tmpdir, config):
        src = LocalSource(config)
        files = src.list_files()
        assert files == []

    def test_list_files_uses_pattern(self, tmpdir, config):
        Config({"source": {"type": "local", "path": tmpdir, "file_pattern": "a*.txt"}})
        c = Config({"source": {"type": "local", "path": tmpdir, "file_pattern": "a*.txt"}})
        p1 = os.path.join(tmpdir, "afile.txt")
        p2 = os.path.join(tmpdir, "bfile.txt")
        for p in (p1, p2):
            with open(p, "w") as f:
                f.write("x")
        src = LocalSource(c)
        files = src.list_files()
        assert len(files) == 1
        assert files[0] == p1

    def test_read_file_returns_bytes(self, tmpdir, config):
        content = b"raw bytes here"
        path = os.path.join(tmpdir, "data.txt")
        with open(path, "wb") as f:
            f.write(content)
        src = LocalSource(config)
        assert src.read_file(path) == content

    def test_list_files_sorted(self, tmpdir, config):
        Config({"source": {"type": "local", "path": tmpdir, "file_pattern": "*"}})
        c = Config({"source": {"type": "local", "path": tmpdir, "file_pattern": "*"}})
        for name in ("z.txt", "a.txt", "m.txt"):
            with open(os.path.join(tmpdir, name), "w") as f:
                f.write("x")
        src = LocalSource(c)
        files = src.list_files()
        assert files == sorted(files)

    def test_type_name(self):
        assert LocalSource.type_name == "local"

    def test_register(self, tmpdir):
        from factories import Factory
        cfg = Config({"source": {"type": "local", "path": tmpdir}})
        f = Factory(cfg, register_defaults=False)
        src = LocalSource(cfg)
        src.register(f)
        assert "local" in f._registry


# --- S3Source ---

class TestS3Source:

    @pytest.fixture
    def config(self):
        return Config({
            "source": {"type": "s3", "path": "prefix/data/", "file_pattern": "*.csv",
                        "s3": {"bucket": "my-bucket"}},
        })

    def test_list_files_filters_by_prefix(self, config):
        mock_resp = {
            "Contents": [
                {"Key": "prefix/data/a.csv"},
                {"Key": "prefix/data/b.csv"},
                {"Key": "prefix/data/c.json"},
            ]
        }
        mock_client = mock.Mock()
        mock_client.list_objects_v2.return_value = mock_resp

        src = S3Source(config, region="us-east-1", bucket="my-bucket")
        src._client = mock_client
        files = src.list_files()
        assert files == ["prefix/data/a.csv", "prefix/data/b.csv"]

    def test_read_file(self, config):
        mock_body = mock.Mock()
        mock_body.read.return_value = b"csv data"
        mock_resp = {"Body": mock_body}
        mock_client = mock.Mock()
        mock_client.get_object.return_value = mock_resp

        src = S3Source(config, region="us-east-1", bucket="my-bucket")
        src._client = mock_client
        data = src.read_file("prefix/data/file.csv")
        assert data == b"csv data"

    def test_client_lazy_init(self, config):
        src = S3Source(config, region="us-east-1", bucket="my-bucket")
        assert src._client is None
        _ = src.client
        assert src._client is not None

    def test_type_name(self):
        assert S3Source.type_name == "s3"


# --- SourceBase ---

class _ConcreteSource(SourceBase):
    """Concrete subclass for testing read_file_df."""
    def list_files(self):
        return []
    def read_file(self, path):
        return b"name,age\nAlice,30"

    def read_file_df(self, path):
        import io
        return pd.read_csv(io.BytesIO(self.read_file(path)))


class TestSourceBase:

    def test_abstract_cannot_instantiate(self):
        with pytest.raises(TypeError):
            SourceBase()

    def test_read_file_df_returns_dataframe(self):
        src = _ConcreteSource(Config({}))
        df = src.read_file_df("x")
        assert isinstance(df, pd.DataFrame)
