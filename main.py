"""CLI entry point for ingestionator."""

import argparse
import logging
import sys

from config import Config, ConfigurationError, load
from factories import Factory
from pipeline import Pipeline
from secret_manager import SecretManager


def setup_logging(config: Config) -> None:
    """Configure Python logging from config."""
    kwargs: dict = {
        "level": getattr(logging, config.get("logging.level", "INFO")),
        "format": config.get("logging.format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
    }
    log_file = config.get("logging.file")
    if log_file:
        import os
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        kwargs["filename"] = log_file
    logging.basicConfig(**kwargs)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ingestionator — data ingestion pipeline")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML (default: config.yaml)")
    parser.add_argument("--file", help="Ingest a single file instead of the configured source")
    parser.add_argument("--source-type", help="Override source.type from config")
    parser.add_argument("--target-type", help="Override target.type from config")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    # Load config
    try:
        cfg = load(args.config)
    except ConfigurationError as e:
        print(f"Config error: {e}", file=sys.stderr)
        return 2

    # Save CLI overrides before they're lost on rehydration
    _src_override = args.source_type or None
    _tgt_override = args.target_type or None

    # Resolve secrets (mutates cfg._raw in place via resolve_config)
    secrets_cfg = cfg.get_section("secrets")
    if secrets_cfg.get("aws", {}).get("enabled"):
        region = secrets_cfg["aws"].get("region")
        secret_name = secrets_cfg["aws"].get("secret_name")
        env_prefix = secrets_cfg.get("env_prefix", "INGESTIONATOR")
        sm = SecretManager(env_prefix=env_prefix, region=region, secret_name=secret_name)
        cfg._raw = sm.resolve_config(cfg._raw)

    cfg.validate()

    # Re-apply CLI overrides after secret resolution
    if _src_override:
        cfg._raw.setdefault("source", {})["type"] = _src_override
    if _tgt_override:
        cfg._raw.setdefault("target", {})["type"] = _tgt_override

    cfg.validate()
    setup_logging(cfg)

    # Create factory
    factory = Factory(cfg)

    # Run pipeline
    pipeline = Pipeline(cfg, factory)

    try:
        rows = pipeline.run_file(args.file) if args.file else pipeline.run()
        print(f"Ingested {rows} rows")
        return 0
    except Exception as e:
        print(f"Pipeline failed: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
