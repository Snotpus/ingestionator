# Contributing: Adding New Connectors

This guide explains how to add new data sources, file format ingestors, or database targets to ingestionator.

## Overview

All connectors follow the same pattern:

1. Create a class inheriting from the appropriate base class
2. Set `type_name` to a unique string identifier
3. Implement the required abstract methods
4. Register the type with the factory via a `register()` call

## Adding a New Source

### 1. Create the class

```python
# sources/gcs.py
from sources.base import SourceBase

class GcsSource(SourceBase):
    type_name = "gcs"

    def __init__(self, config):
        super().__init__(config)
        # Initialize GCS client, bucket, etc.

    def list_files(self) -> list[str]:
        # Return list of GCS object keys
        ...

    def read_file(self, path: str) -> bytes:
        # Read and return raw bytes from GCS
        ...
```

### 2. Register it

```python
# sources/__init__.py
from .gcs import GcsSource

def setup_sources(factory):
    # Built-in sources
    from .local import LocalSource
    from .s3 import S3Source
    LocalSource({"type": "local", "path": "."}).register(factory)
    S3Source({"type": "s3", "path": ""}).register(factory)

    # Custom sources
    GcsSource({"type": "gcs", "path": ""}).register(factory)
```

### 3. Use it

```yaml
source:
  type: gcs
  path: my-bucket/data/
```

## Adding a New Ingestor

### 1. Create the class

```python
# ingestors/json.py
import io
import pandas as pd
from ingestors.base import IngestorBase

class JsonIngestor(IngestorBase):
    type_name = "json"

    def __init__(self, config):
        super().__init__(config)
        # Read config options (e.g., orient, encoding)

    def ingest(self, data: bytes) -> pd.DataFrame:
        return pd.read_json(io.BytesIO(data), orient="records")
```

### 2. Register it

```python
# ingestors/__init__.py
from .json import JsonIngestor

def setup_ingestors(factory):
    from .csv import CSVIngestor
    from .parquet import ParquetIngestor
    CSVIngestor({"type": "csv"}).register(factory)
    ParquetIngestor({"type": "parquet"}).register(factory)
    JsonIngestor({"type": "json"}).register(factory)
```

### 3. Use it

```yaml
ingestor:
  type: json
```

## Adding a New Target

### 1. Create the class

```python
# targets/postgres.py
import pandas as pd
from targets.base import TargetBase

class PostgresTarget(TargetBase):
    type_name = "postgres"

    def __init__(self, config):
        super().__init__(config)
        # Parse connection params from config

    def write(self, df: pd.DataFrame) -> None:
        # Use psycopg2 or similar to insert DataFrame
        ...

    def read(self) -> pd.DataFrame:
        # Query and return DataFrame
        ...
```

### 2. Register it

```python
# targets/__init__.py
from .postgres import PostgresTarget

def setup_targets(factory):
    from .duckdb import DuckDBStorage
    from .snowflake import SnowflakeStorage
    DuckDBStorage({"type": "duckdb"}).register(factory)
    SnowflakeStorage({"type": "snowflake"}).register(factory)
    PostgresTarget({"type": "postgres"}).register(factory)
```

### 3. Use it

```yaml
target:
  type: postgres
  database: postgresql://user:pass@host/db
  table: ingested_data
  mode: append
```

## Full Example: JSON Ingestor

Here is a complete, runnable JSON ingestor:

### `ingestors/json.py`

```python
import io
from typing import Any

import pandas as pd

from ingestors.base import IngestorBase


class JsonIngestor(IngestorBase):
    """Parse JSON file data into a DataFrame."""

    type_name = "json"

    def __init__(self, config: Any):
        super().__init__(config)
        self.encoding = config.get("encoding", "utf-8")
        self.orient = config.get("orient", "records")

    def ingest(self, data: bytes) -> pd.DataFrame:
        return pd.read_json(io.BytesIO(data), orient=self.orient, encoding=self.encoding)
```

### Register in `ingestors/__init__.py`

```python
from .json import JsonIngestor

def setup_ingestors(factory):
    from .csv import CSVIngestor
    from .parquet import ParquetIngestor
    CSVIngestor({"type": "csv"}).register(factory)
    ParquetIngestor({"type": "parquet"}).register(factory)
    JsonIngestor({"type": "json", "orient": "records"}).register(factory)
```

### Config

```yaml
ingestor:
  type: json
  json:
    orient: records
    encoding: utf-8
```

## Testing

Mirror the existing test structure in `tests/`:

```python
# tests/test_json_ingestor.py
import pytest
from ingestors.json import JsonIngestor


def test_json_ingestor():
    data = b'[{"a": 1}, {"a": 2}]'
    ingestor = JsonIngestor({"type": "json", "orient": "records"})
    df = ingestor.ingest(data)
    assert len(df) == 2
    assert df["a"].tolist() == [1, 2]


def test_json_ingestor_type_name():
    assert JsonIngestor.type_name == "json"
```

Run tests:

```bash
./venv/bin/pytest tests/
```

## Checklist for New Connectors

- [ ] Inherits from the correct base class (`SourceBase`, `IngestorBase`, or `TargetBase`)
- [ ] `type_name` is set and unique within its category
- [ ] All abstract methods are implemented
- [ ] Connector is registered in `setup_*()` in the package `__init__.py`
- [ ] `config.yaml` example shows the new type
- [ ] Tests added in `tests/`
- [ ] README updated with the new type in the configuration reference
