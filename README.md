# Ingestionator

A flexible data ingestion framework with a factory-based extension model for reading from multiple sources, parsing various file formats, and writing to different targets.

```
                    ┌─────────────┐
                    │ config.yaml │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │  Config     │ ← load, validate, resolve secrets
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
       ─ ─ ─ ─ ─ ─ ─┤  Factory   │ ← registry pattern
                    └──┬───┬─────┘
               ┌───────┘   └───────┐
          ┌────▼────┐      ┌───────▼──────┐
          │ Sources │      │   Targets    │
          │ local/  │      │  DuckDB      │
          │   s3    │      │  Snowflake   │
          └─────────┘      └──────────────┘
                           ┌───────▼──────┐
                           │  Ingestors   │
                           │   CSV/Parquet│
                           └──────────────┘
```

## Features

- **Multiple sources**: local filesystem, S3
- **Multiple formats**: CSV, Parquet
- **Multiple targets**: DuckDB (local SQLite-like), Snowflake (cloud data warehouse)
- **Secret management**: environment variables or AWS Secrets Manager (`$SECRET:` syntax)
- **Configurable retry**: exponential backoff with configurable attempts
- **Factory-based extensions**: add new sources, ingestors, or targets with minimal code

## Quick Start

### 1. Install dependencies

Production:

```bash
pip install -r requirements.txt
```

Development (includes testing and linting tools):

```bash
pip install -e ".[dev]"
```

All core packages are also listed in `pyproject.toml`.

### 2. Configure

Edit `config.yaml` (see [Configuration Reference](#configuration-reference) below):

```yaml
source:
  type: local
  path: ./test_data
  file_pattern: "*.csv"

target:
  type: duckdb
  database: ./output/ingested.db
  table: ingested_data
  mode: replace

ingestor:
  type: csv
```

### 3. Run

```bash
# Ingest all files matching the config
python main.py

# Ingest a single file
python main.py --file data.csv

# Override source type from CLI
python main.py --source-type s3

# Override target type from CLI
python main.py --target-type snowflake
```

Exit codes: `0` = success, `1` = pipeline error, `2` = config/usage error.

## Configuration Reference

### Pipeline

| Key | Default | Description |
|-----|---------|-------------|
| `pipeline.name` | `ingestionator` | Pipeline name identifier |
| `pipeline.version` | `"0.1"` | Pipeline version |

### Source

| Key | Default | Description |
|-----|---------|-------------|
| `source.type` | `local` | Source type: `local` or `s3` |
| `source.path` | *(required)* | Path to source files (local) or S3 prefix |
| `source.file_pattern` | `*` | Glob pattern for file matching. Use `**/*.csv` for recursive matching into subdirectories |

### Ingestor

| Key | Default | Description |
|-----|---------|-------------|
| `ingestor.type` | `csv` | Ingestor type: `csv` or `parquet` |
| `ingestor.csv.delimiter` | `,` | CSV delimiter character |
| `ingestor.csv.encoding` | `utf-8` | File encoding |
| `ingestor.csv.header` | `0` | Row number for header |
| `ingestor.csv.skip_blank_lines` | `true` | Skip blank lines |
| `ingestor.parquet.engine` | `pyarrow` | Parquet engine |
| `ingestor.parquet.columns` | `null` | Columns to read |
| `ingestor.parquet.filters` | `null` | Row group filters |

### Target

| Key | Default | Description |
|-----|---------|-------------|
| `target.type` | `duckdb` | Target type: `duckdb` or `snowflake` |
| `target.database` | `./output/ingested.db` | Database path (DuckDB) or connection string (Snowflake) |
| `target.table` | `ingested_data` | Default table name (used when no file_to_table mapping) |
| `target.mode` | `replace` | Write mode: `replace` or `append` |

### File to Table Mapping

By default every file writes to the same `target.table`. Use `file_to_table` to send each source file to its own table. Keys can be filenames or directory names:

| Key | Type | Description |
|-----|------|-------------|
| `file_to_table.default` | string | Fallback table name for unmapped files |
| `file_to_table.<filename>` | string | Table name for a specific file (e.g., `users.csv`) |
| `file_to_table.<dir>` | string | Table name for all files in a directory (e.g., `customers`) |

Matching priority (deepest first):
1. **Exact relative path match** — e.g., `customers/datafile.csv`
2. **Basename match** — e.g., `datafile.csv`
3. **Directory name match** — matches any parent directory (deepest first). e.g., `customers/20240112/datafile.csv` → `customers`
4. **Default fallback** — if no key matches

```yaml
file_to_table:
  default: ingested_data
  users.csv: users
  events.csv: events
  products.csv: products
  customers: customers_table
```

Files not listed fall back to `default`. Remove the `file_to_table` block entirely to restore the single-table behavior.

#### Example: Multi-level directory mapping

Given this config:
```yaml
file_to_table:
  default: raw
  orders.csv: orders
  customers: customers
```

| File Path | Resolves To |
|-----------|-------------|
| `users.csv` | `users` (no match, falls to `default: raw`) |
| `customers/20240112/file1.csv` | `customers_table` (directory match) |
| `customers/20240212/file2.csv` | `customers_table` (directory match) |
| `orders/20240112/orders.csv` | `orders` (exact match) |
| `other/unknown.csv` | `raw` (default) |

> **Tip:** Use `**/*.csv` as your `file_pattern` to recursively discover files in subdirectories.

### Secrets

| Key | Default | Description |
|-----|---------|-------------|
| `secrets.aws.enabled` | `false` | Enable AWS Secrets Manager |
| `secrets.aws.region` | `us-east-1` | AWS region |
| `secrets.aws.secret_name` | `ingestionator/secrets` | Secret ID in AWS |
| `secrets.env_prefix` | `INGESTIONATOR` | Env var prefix for secrets |

Secrets are referenced in config as `$SECRET:ref_name`. Resolution order: environment variable (`{PREFIX}_{ref_name}`), then AWS Secrets Manager.

### Error Handling

| Key | Default | Description |
|-----|---------|-------------|
| `error_handling.retry_attempts` | `3` | Number of retry attempts |
| `error_handling.backoff_factor` | `2` | Multiplier for exponential backoff |
| `error_handling.max_delay` | `300` | Maximum delay between retries (seconds) |

### Logging

| Key | Default | Description |
|-----|---------|-------------|
| `logging.level` | `INFO` | Python logging level |
| `logging.format` | `%(asctime)s - %(name)s - %(levelname)s - %(message)s` | Log format string |
| `logging.file` | `./output/pipeline.log` | Log file path |

## CLI Reference

| Flag | Description |
|------|-------------|
| `--config PATH` | Path to config YAML (default: `config.yaml`) |
| `--file PATH` | Ingest a single file instead of the configured source |
| `--source-type TYPE` | Override `source.type` from config (`local`, `s3`) |
| `--target-type TYPE` | Override `target.type` from config (`duckdb`, `snowflake`) |

## Usage Examples

### Local CSV to DuckDB (default)

```yaml
source:
  type: local
  path: ./data
  file_pattern: "*.csv"

target:
  type: duckdb
  database: ./output/data.db
  table: events
  mode: append
```

```bash
python main.py
```

### Per-file table mapping

```yaml
source:
  type: local
  path: ./data
  file_pattern: "*.csv"

target:
  type: duckdb
  database: ./output/data.db
  table: raw      # fallback table for unmapped files

file_to_table:
  default: raw
  users.csv: users
  events.csv: events
  products.csv: products
```

`users.csv` writes to the `users` table, `events.csv` to `events`, and any unmapped file goes to `raw`. Each file gets its own separate table — no more overwriting.

### S3 CSV to Snowflake

```yaml
source:
  type: s3
  path: my-bucket/data/ingest
  file_pattern: "*.csv"

target:
  type: snowflake
  database: ./output/snowflake_conn.yaml
  table: raw_events
  mode: append

secrets:
  aws:
    enabled: true
    region: us-west-2
```

```bash
SNOWFLAKE_ACCOUNT=myaccount.snowflakecomputing.com python main.py
```

### Single file ingestion

```bash
python main.py --file data/special.csv --target-type duckdb
```

## Architecture

The pipeline uses a factory-based extension model:

1. **Config** loads and validates `config.yaml`, resolving `$SECRET:` references
2. **Factory** maintains a registry of source, ingestor, and target types
3. **Sources** discover and read files (`LocalSource`, `S3Source`)
4. **Ingestors** parse file bytes into DataFrames (`CSVIngestor`, `ParquetIngestor`)
5. **Targets** write DataFrames to storage (`DuckDBStorage`, `SnowflakeStorage`)

Each layer is independently extensible. See [ARCHITECTURE.md](ARCHITECTURE.md) for design details and [CONTRIBUTING.md](CONTRIBUTING.md) for adding new connectors.

## Project Structure

```
config.yaml            Pipeline configuration
config.py              Configuration loader and validator
pyproject.toml         Project metadata, deps, tool config
factories.py           Factory registry pattern
secret_manager.py      Secret resolution (env + AWS)
pipeline.py            Pipeline orchestrator
error_handling.py      Retry logic and custom exceptions
main.py                CLI entry point
sources/               Data source connectors
  base.py              SourceBase abstract class
  local.py             Local filesystem source
  s3.py                S3 source
ingestors/             File format processors
  base.py              IngestorBase abstract class
  csv.py               CSV parser
  parquet.py           Parquet parser
targets/               Database targets
  base.py              TargetBase abstract class
  duckdb.py            DuckDB storage
  snowflake.py         Snowflake storage
tests/                 Test suite
test_data/             Sample data
output/                Generated output
```

## Dependencies

| Package | Purpose |
|---------|---------|
| `pyyaml` | YAML config parsing |
| `pandas` | DataFrame representation |
| `duckdb` | Local database target |
| `pyarrow` | Parquet file format support |
| `boto3` | AWS S3 / Secrets Manager integration |

### Dev Dependencies

| Package | Purpose |
|---------|---------|
| `pytest` | Test runner |
| `ruff` | Linter and formatter |
| `setuptools` | Build backend |

## Testing

Run the test suite:

```bash
pytest
```

With coverage:

```bash
pytest --cov=.
```

The project has 119 tests across 7 test files covering config, sources, ingestors, targets, pipeline, and integration flows.

## Linting

Run the linter:

```bash
ruff check .
```

Auto-fix:

```bash
ruff check . --fix
```

## CI

This project uses GitHub Actions. CI runs on every push and pull request to `main` or `master`:

- **pytest** across Python 3.10, 3.11, 3.12, 3.13
- **ruff** lint check

## License

MIT.
