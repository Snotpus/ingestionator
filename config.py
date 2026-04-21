"""Configuration loader that reads, validates, and provides typed access to pipeline config."""

from __future__ import annotations

from typing import Any

import yaml

SOURCE_DEFAULTS = {"type": "local", "path": "./test_data", "file_pattern": "*"}
TARGET_DEFAULTS = {"type": "duckdb", "database": "./output/ingested.db", "table": "ingested_data", "mode": "replace"}

_CONFIG_DEFAULTS: dict[str, Any] = {
    "ingestor": {"csv": {"delimiter": ",", "encoding": "utf-8"}, "parquet": {"engine": "pyarrow"}},
    "error_handling": {"retry_attempts": 3, "backoff_factor": 2},
    "logging": {"level": "INFO"},
}


class ConfigurationError(Exception):
    """Raised when configuration is invalid or cannot be loaded."""

    def __init__(self, message: str, key: str = ""):
        self.key = key
        super().__init__(f"ConfigurationError: {message}" if not key else f"ConfigurationError: [{key}] {message}")


class Config:
    """Wraps raw config dict with typed access and validation."""

    _DEFAULTS: dict[str, Any] = _CONFIG_DEFAULTS

    VALID_SOURCES = frozenset(("local", "s3"))
    VALID_TARGETS = frozenset(("duckdb", "snowflake"))

    def __init__(self, raw: dict[str, Any]):
        self._raw = dict(raw)
        # Apply defaults for non-source/target keys
        for key, default_val in self._DEFAULTS.items():
            if key not in self._raw:
                self._raw[key] = default_val

    def get(self, dotted_key: str, default: Any = None) -> Any:
        """Access config via dot-notation (e.g. get("source.type"))."""
        keys = dotted_key.split(".")
        current = self._raw
        for k in keys:
            if isinstance(current, dict) and k in current:
                current = current[k]
            else:
                return default
        return current

    def get_section(self, section: str) -> dict[str, Any]:
        """Get a top-level config section."""
        return self.get(section, {})

    def validate(self, required_sections: str | list | None = None) -> None:
        """Validate the config. Raises ConfigurationError on failure."""
        if required_sections is None:
            required_sections = ["source", "target"]

        if isinstance(required_sections, str):
            required_sections = [required_sections]

        # Apply defaults for missing required sections
        for section in required_sections:
            if section not in self._raw:
                if section == "source":
                    self._raw["source"] = dict(SOURCE_DEFAULTS)
                elif section == "target":
                    self._raw["target"] = dict(TARGET_DEFAULTS)
                else:
                    raise ConfigurationError(f"Missing required section: {section}", key=section)

        # Use shared defaults for non-source/target sections
        for key, default_val in self._DEFAULTS.items():
            if key not in self._raw:
                self._raw[key] = default_val

        # Validate source
        self._raw.setdefault("source", {})
        self._raw["source"].setdefault("type", "local")
        self._raw["source"].setdefault("path", "./test_data")
        self._raw["source"].setdefault("file_pattern", "*")

        source_type = self._raw["source"]["type"]
        if source_type not in self.VALID_SOURCES:
            raise ConfigurationError(
                f"Invalid source type: {source_type!r}. Must be one of: {sorted(self.VALID_SOURCES)}",
                key="source.type",
            )

        # Validate target
        self._raw.setdefault("target", {})
        self._raw["target"].setdefault("type", "duckdb")
        self._raw["target"].setdefault("database", "./output/ingested.db")
        self._raw["target"].setdefault("table", "ingested_data")
        self._raw["target"].setdefault("mode", "replace")

        target_type = self._raw["target"]["type"]
        if target_type not in self.VALID_TARGETS:
            raise ConfigurationError(
                f"Invalid target type: {target_type!r}. Must be one of: {sorted(self.VALID_TARGETS)}",
                key="target.type",
            )

        # Validate source has a path
        if not self.get("source.path"):
            raise ConfigurationError("source.path is required", key="source.path")

    def validate_all(self) -> None:
        """Full validation (all sections)."""
        return self.validate()

    def to_dict(self) -> dict[str, Any]:
        """Return the raw config dict."""
        return dict(self._raw)

    def __repr__(self) -> str:
        src = self.get("source.type")
        tgt = self.get("target.type")
        return f"Config(source={src}, target={tgt})"


def load(path: str = "config.yaml") -> Config:
    """Load config from a YAML file and return a Config instance."""
    try:
        with open(path) as f:
            raw = yaml.safe_load(f)
    except FileNotFoundError:
        raise ConfigurationError(f"Config file not found: {path}", key="file")
    except yaml.YAMLError as e:
        raise ConfigurationError(f"Invalid YAML: {e}", key="file")

    if not isinstance(raw, dict):
        raise ConfigurationError("Config must be a YAML mapping", key="file")

    return Config(raw)


def load_config(path: str = "config.yaml") -> Config:
    """Module-level convenience: load + validate.

    Deprecated: Prefer calling ``load(path)`` and ``config.validate()`` separately.
    """
    config = load(path)
    config.validate()
    return config
