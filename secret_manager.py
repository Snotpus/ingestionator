"""Secret manager that resolves $SECRET:ref_name references from environment variables or AWS Secrets Manager."""

from __future__ import annotations

import os
import re
from typing import Any

import boto3

_SECRET_PATTERN = re.compile(r"^\$SECRET:([\w.]+)$")


class SecretError(Exception):
    """Raised when a secret cannot be resolved."""

    def __init__(self, ref: str, hint: str | None = None):
        msg = f"Secret not found: {ref}"
        if hint:
            msg += f" — {hint}"
        super().__init__(msg)


class SecretManager:
    """Resolves $SECRET:ref_name references from environment variables or AWS Secrets Manager."""

    def __init__(self, env_prefix: str = "INGESTIONATOR", region: str | None = None,
                 secret_name: str | None = None):
        self._env_prefix = env_prefix
        self._region = region
        self._secret_name = secret_name or "ingestionator/secrets"
        self._resolved: dict[str, str] = {}
        self._secrets_client: Any | None = None

    def resolve(self, value: str) -> str:
        """Resolve a secret reference. Returns value unchanged if not a secret pattern."""
        match = _SECRET_PATTERN.match(value)
        if not match:
            return value

        ref = match.group(1)

        # Check cache
        if ref in self._resolved:
            return self._resolved[ref]

        # Check environment variable
        env_key = f"{self._env_prefix}_{ref}"
        env_val = os.environ.get(env_key)
        if env_val:
            self._resolved[ref] = env_val
            return env_val

        # Check AWS Secrets Manager
        if self._region:
            return self._resolve_from_aws(ref)

        hint = (f"Set {env_key} in your environment or enable AWS Secrets Manager in config")
        raise SecretError(ref, hint)

    @property
    def _secrets_manager(self):
        """Lazy initialize AWS Secrets Manager client."""
        if self._secrets_client is None and self._region:
            self._secrets_client = boto3.client("secretsmanager", region_name=self._region)
        return self._secrets_client

    def _resolve_from_aws(self, ref: str) -> str:
        """Resolve a secret from AWS Secrets Manager."""
        client = self._secrets_manager
        if not client:
            hint = "AWS Secrets Manager is not configured (no region set)"
            raise SecretError(ref, hint)

        secret = client.get_secret_value(SecretId=self._secret_name)
        stored: dict[str, str] = self._parse_secret_string(secret["SecretString"])
        val = stored.get(ref)
        if val is None:
            hint = f"Key '{ref}' not found in AWS secret '{self._secret_name}'"
            raise SecretError(ref, hint)
        self._resolved[ref] = val
        return val

    @staticmethod
    def _parse_secret_string(s: str) -> dict[str, Any]:
        """Parse a JSON or plain-key-value secret string."""
        import json
        try:
            data = json.loads(s)
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError:
            pass
        # Fallback: plain key=value format
        result: dict[str, Any] = {}
        for line in s.strip().splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                result[k.strip()] = v.strip()
        return result

    def resolve_config(self, config: dict[str, Any]) -> dict[str, Any]:
        """Recursively walk a config dict resolving all $SECRET:ref references."""
        if isinstance(config, dict):
            return {k: self.resolve_config(v) for k, v in config.items()}
        if isinstance(config, list):
            return [self.resolve_config(item) for item in config]
        if isinstance(config, str):
            return self.resolve(config)
        return config


def resolve_config(config: dict[str, Any], prefix: str = "INGESTIONATOR") -> dict[str, Any]:
    """Module-level convenience for resolving secrets in a config dict."""
    return SecretManager(env_prefix=prefix).resolve_config(config)
