"""Tests for secret_manager.py — SecretManager class and resolve_config convenience."""

import os
from unittest import mock

import pytest

from secret_manager import SecretError, SecretManager, resolve_config

# --- env var resolution ---

class TestSecretManagerEnv:

    def test_resolve_returns_plain_value_unmodified(self):
        sm = SecretManager()
        assert sm.resolve("not_a_secret") == "not_a_secret"

    def test_resolve_returns_secret_from_env(self):
        sm = SecretManager(env_prefix="TEST_SM")
        os.environ["TEST_SM_DB_PASSWORD"] = "hunter2"
        try:
            assert sm.resolve("$SECRET:DB_PASSWORD") == "hunter2"
        finally:
            del os.environ["TEST_SM_DB_PASSWORD"]

    def test_resolve_raises_on_missing_secret(self):
        sm = SecretManager(env_prefix="TEST_SM")
        assert "TEST_SM_NOPE" not in os.environ
        with pytest.raises(SecretError, match="NOPE"):
            sm.resolve("$SECRET:NOPE")

    def test_resolve_uses_cache(self):
        sm = SecretManager(env_prefix="TEST_SM_CACHE")
        os.environ["TEST_SM_CACHE_X"] = "val1"
        assert sm.resolve("$SECRET:X") == "val1"
        # Update env — cache should still return old value
        os.environ["TEST_SM_CACHE_X"] = "val2"
        assert sm.resolve("$SECRET:X") == "val1"

    def test_resolve_clears_cache_between_instances(self):
        sm = SecretManager(env_prefix="TEST_SM")
        os.environ["TEST_SM_Y"] = "first"
        assert sm.resolve("$SECRET:Y") == "first"
        # New instance has no cache
        sm2 = SecretManager(env_prefix="TEST_SM")
        os.environ["TEST_SM_Y"] = "second"
        assert sm2.resolve("$SECRET:Y") == "second"


# --- AWS Secrets Manager ---

class TestSecretManagerAWS:

    def test_resolve_from_aws(self):
        sm = SecretManager(env_prefix="TEST_SM_AWS", region="us-west-2")
        mock_secret = '{"db_pass": "aws_secret_val"}'
        mock_client = mock.Mock()
        mock_client.get_secret_value.return_value = {"SecretString": mock_secret}
        sm._secrets_client = mock_client
        result = sm.resolve("$SECRET:db_pass")
        assert result == "aws_secret_val"

    def test_resolve_from_aws_raises_on_missing_key(self):
        sm = SecretManager(env_prefix="TEST_SM_AWS", region="us-west-2")
        mock_secret = '{"other_key": "val"}'
        mock_client = mock.Mock()
        mock_client.get_secret_value.return_value = {"SecretString": mock_secret}
        sm._secrets_client = mock_client
        with pytest.raises(SecretError, match="Key 'db_pass' not found"):
            sm.resolve("$SECRET:db_pass")

    def test_resolve_falls_back_to_aws_when_no_env(self):
        sm = SecretManager(env_prefix="TEST_SM_AWS", region="us-west-2")
        mock_secret = '{"pass": "from_aws"}'
        mock_client = mock.Mock()
        mock_client.get_secret_value.return_value = {"SecretString": mock_secret}
        sm._secrets_client = mock_client
        result = sm.resolve("$SECRET:pass")
        assert result == "from_aws"

    def test_no_region_raises_hint(self):
        sm = SecretManager(region=None)  # no region = no AWS
        with pytest.raises(SecretError, match="Set .* in your environment"):
            sm.resolve("$SECRET:must_be_in_env")


# --- _parse_secret_string ---

class TestParseSecretString:

    def test_parse_json(self):
        data = SecretManager._parse_secret_string('{"a": "1", "b": "2"}')
        assert data == {"a": "1", "b": "2"}

    def test_parse_kv(self):
        data = SecretManager._parse_secret_string("a=1\nb=2\n")
        assert data == {"a": "1", "b": "2"}

    def test_parse_invalid_json_fallback(self):
        data = SecretManager._parse_secret_string("not json\na=b")
        assert data == {"a": "b"}


# --- resolve_config (recursive) ---

class TestResolveConfig:

    def test_resolve_config_walks_dict(self):
        sm = SecretManager(env_prefix="TEST_RC")
        os.environ["TEST_RC_KEY"] = "resolved"
        try:
            cfg = {"db": {"password": "$SECRET:KEY"}}
            result = sm.resolve_config(cfg)
        finally:
            del os.environ["TEST_RC_KEY"]
        assert result["db"]["password"] == "resolved"

    def test_resolve_config_walks_list(self):
        sm = SecretManager(env_prefix="TEST_RC")
        os.environ["TEST_RC_VAL"] = "in_list"
        try:
            cfg = {"secrets": ["$SECRET:VAL", "plain"]}
            result = sm.resolve_config(cfg)
        finally:
            del os.environ["TEST_RC_VAL"]
        assert result["secrets"] == ["in_list", "plain"]

    def test_resolve_config_leaves_non_strings(self):
        sm = SecretManager()
        cfg = {"num": 42, "bool": True, "null": None}
        result = sm.resolve_config(cfg)
        assert result == {"num": 42, "bool": True, "null": None}

    def test_resolve_config_convenience(self):
        os.environ["TEST_RC_CONV_KEY"] = "yes"
        try:
            result = resolve_config({"k": "$SECRET:KEY"}, prefix="TEST_RC_CONV")
        finally:
            del os.environ["TEST_RC_CONV_KEY"]
        assert result["k"] == "yes"
