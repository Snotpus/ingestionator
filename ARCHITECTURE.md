# Architecture

This document explains the design decisions and data flow of the ingestionator pipeline.

## Data Flow

```
  [Source]            [Ingestor]           [Target]
  ┌────────┐          ┌─────────┐          ┌─────────┐
  │ discover │────bytes──▶│ parse │───DataFrame──▶│ persist │
  │   files  │  (raw)   │ format │  (pd.DataFrame)│  data  │
  └────────┘          └─────────┘          └─────────┘
       │                     │                     │
  list_files()          ingest(bytes)          write(df)
  read_file(path)                            read()
```

1. **Source** discovers files via `list_files()` and reads raw bytes via `read_file(path)`
2. **Ingestor** parses raw bytes into a `pandas.DataFrame` via `ingest(data)`
3. **Target** persists the DataFrame via `write(df)`

These three stages are chained in `Pipeline.run()`:

```python
for path in source.list_files():
    data = source.read_file(path)
    df = ingestor.ingest(data)
    target.write(df)
```

## Factory Pattern

The `Factory` class (`factories.py`) is the central registry. It maps type strings to instantiation functions:

```python
factory = Factory(config)
source = factory.create("local")   # calls registered factory fn with config
ingestor = factory.create("csv")
target = factory.create("duckdb")
```

### Registration Flow

Each package (`sources/`, `ingestors/`, `targets/`) exports a `setup_<name>(factory)` function in its `__init__.py`. The factory calls all three during `_register_defaults()`:

```python
# factories.py _register_defaults()
from sources import setup_sources
from ingestors import setup_ingestors
from targets import setup_targets
setup_sources(self)
setup_ingestors(self)
setup_targets(self)
```

Each `setup_*` function registers its types by instantiating a connector and calling its `register(factory)` method:

```python
# Example from sources/__init__.py
def setup_sources(factory):
    src = LocalSource({"type": "local", "path": "."})
    src.register(factory)  # registers "local"
```

The `register()` method on each base class stores a lambda that reconstructs the class later:

```python
factory.register(self.type_name, lambda cfg: self.__class__(cfg))
```

### Adding Custom Types

You can register additional types after creating a factory:

```python
factory = Factory(config, register_defaults=False)  # skip defaults
factory.register("my_source", lambda cfg: MySource(cfg))
```

Or call `setup_*` manually for fine-grained control:

```python
factory = Factory(config, register_defaults=False)
setup_sources(factory)  # register built-in sources only
factory.register("my_source", lambda cfg: MySource(cfg))
```

## Extension Points

### SourceBase (`sources/base.py`)

```python
class SourceBase(ABC):
    type_name: str = ""

    def __init__(self, config: Config): ...

    @abstractmethod
    def list_files(self) -> List[str]: ...

    @abstractmethod
    def read_file(self, path: str) -> bytes: ...

    def register(self, factory: Factory) -> None: ...
```

To add a new source (e.g., GCS):
1. Inherit `SourceBase` and set `type_name = "gcs"`
2. Implement `list_files()` and `read_file()`
3. Create a `setup_sources()` function that calls `register()`

### IngestorBase (`ingestors/base.py`)

```python
class IngestorBase(ABC):
    type_name: str = ""

    def __init__(self, config: Config): ...

    @abstractmethod
    def ingest(self, data: bytes) -> pd.DataFrame: ...

    def register(self, factory: Factory) -> None: ...
```

To add a new format (e.g., JSON):
1. Inherit `IngestorBase` and set `type_name = "json"`
2. Implement `ingest()` using `pd.read_json()` or equivalent
3. Register via a `setup_ingestors()` function

### TargetBase (`targets/base.py`)

```python
class TargetBase(ABC):
    type_name: str = ""

    def __init__(self, config: Config): ...

    @abstractmethod
    def write(self, df: pd.DataFrame) -> None: ...

    @abstractmethod
    def read(self) -> pd.DataFrame: ...

    def register(self, factory: Factory) -> None: ...
```

To add a new target (e.g., PostgreSQL):
1. Inherit `TargetBase` and set `type_name = "postgres"`
2. Implement `write()` and `read()`
3. Register via a `setup_targets()` function

## Secret Management

`secret_manager.py` resolves `$SECRET:ref_name` references with a priority chain:

1. **Cache** — previously resolved secrets are cached to avoid redundant lookups
2. **Environment variable** — `{PREFIX}_{ref_name}` (e.g., `INGESTIONATOR_DB_PASSWORD`)
3. **AWS Secrets Manager** — fetches from the configured secret store

```python
# In config.yaml:
database: $SECRET:snowflake_password

# Resolution:
# 1. Check INGESTIONATOR_SNOWFLAKE_PASSWORD env var
# 2. If not found, query AWS Secrets Manager
# 3. Raise SecretError if neither has the value
```

The `resolve_config()` method recursively walks the entire config dict, resolving all secret references.

## Error Handling

`error_handling.py` provides:

- **`IngestionError`** — base exception for all pipeline errors
  - `SourceError`, `IngestorError`, `TargetError` — typed subclasses
- **`retry_with_backoff`** — decorator that retries on `IngestionError` with exponential backoff

```python
@retry_with_backoff(attempts=3, base_delay=1.0, max_delay=300.0)
def _run_file(self, path: str) -> int:
    ...

# On failure:
# Attempt 1 fails → sleep 1s
# Attempt 2 fails → sleep 2s
# Attempt 3 fails → sleep 4s (capped at max_delay)
# Raises the final exception
```

The pipeline decorates `_run_file()` with this decorator, so individual file ingestion retries without affecting other files in the batch.

## Configuration System

`config.py` wraps a YAML dict with:

- **`load()`** — reads YAML file, handles `FileNotFoundError` and `YAMLError`
- **`validate()`** — checks required sections (`source`, `target`), valid type enums, required fields (`source.path`)
- **`get(dotted_key)`** — dot-notation access (`config.get("source.type")`)
- **`_raw`** — internal dict with defaults applied for missing sections

### Validation Flow

1. Load YAML via `yaml.safe_load()`
2. Apply defaults for missing top-level sections
3. Validate `source.type` against `VALID_SOURCES`
4. Validate `target.type` against `VALID_TARGETS`
5. Ensure `source.path` is non-empty
6. Raise `ConfigurationError` with the key path on failure

### Secret Injection Flow

1. Load config (with raw `$SECRET:` strings)
2. If AWS secrets enabled, create `SecretManager`
3. Call `resolve_config()` to recursively replace all `$SECRET:` references
4. Create new `Config` instance with resolved dict

## File-to-Table Mapping

The `_resolve_table()` method in `DuckDBStorage` and `SnowflakeStorage` determines which table a file will be written to based on the filename passed from `pipeline.py`. When processing files from subdirectories (e.g., with `**/*.csv` pattern), the filename is the relative path from the source directory.

Matching priority (deepest first):

1. **Exact relative path** — e.g., `customers/datafile.csv` → matches `customers/datafile.csv` key
2. **Basename** — e.g., `datafile.csv` → matches `datafile.csv` key
3. **Directory names** — walks up parent directories from deepest to shallowest. e.g., `customers/20240112/datafile.csv` checks `20240112`, then `customers`
4. **Default** — falls back to `file_to_table.default`

Example resolution steps for `customers/20240112/datafile.csv`:

```
Step 1: "customers/20240112/datafile.csv" not in mapping
Step 2: "datafile.csv" (basename) not in mapping
Step 3: Check parent dirs from deepest:
        20240112 → not in mapping
        customers → in mapping! → return target name
```

## Key Files

| File | Responsibility |
|------|---------------|
| `config.py` | YAML loading, validation, dot-notation access |
| `secret_manager.py` | `$SECRET:` resolution (env + AWS) |
| `factories.py` | Factory registry, default type registration |
| `pipeline.py` | Orchestrates source → ingestor → target loop |
| `error_handling.py` | `IngestionError` hierarchy, retry decorator |
| `main.py` | CLI, wiring config → factory → pipeline |
| `sources/base.py` | `SourceBase` interface |
| `ingestors/base.py` | `IngestorBase` interface |
| `targets/base.py` | `TargetBase` interface |
