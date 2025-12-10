# iceberg-loader

Utilities for loading data into Iceberg tables using PyArrow. Focus on robust ingestion (messy JSON, schema evolution, idempotent loads, upsert) with a simple facade and configurable defaults.

[![PyPI - Version](https://img.shields.io/pypi/v/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![Coverage](https://img.shields.io/badge/coverage-93%25-brightgreen)](coverage.xml)
[![CI](https://github.com/IvanMatveev/iceberg-loader/actions/workflows/ci.yml/badge.svg)](https://github.com/IvanMatveev/iceberg-loader/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Why iceberg-loader?
- Handles messy JSON: auto-serializes dict/list/mixed fields to strings so writes don’t fail.
- Schema evolution: add columns on the fly (opt-in), preserves field ids.
- Safe writes: append/overwrite, idempotent replace via `replace_filter`, upsert.
- Stream friendly: commit intervals, batches, IPC streams.
- One config (`LoaderConfig`) to set defaults; override per-call if needed.

## Features
- Arrow-first: `pa.Table`, `RecordBatch`, IPC.
- Messy JSON friendly: dict/list/mixed -> JSON strings.
- Schema evolution (opt-in).
- Idempotent replace (`replace_filter`) and upsert.
- Commit interval for long streams.
- Maintenance helpers (expire snapshots).

## Quickstart
```bash
pip install "iceberg-loader[all]"
```
```python
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from iceberg_loader import LoaderConfig, load_data_to_iceberg

catalog = load_catalog("default")
data = pa.Table.from_pydict({"id": [1, 2], "signup_date": ["2023-01-01", "2023-01-02"]})

config = LoaderConfig(write_mode="append", partition_col="signup_date", schema_evolution=True)
load_data_to_iceberg(data, ("db", "users"), catalog, config=config)
```

## Compatibility
- Python: 3.10, 3.11, 3.12, 3.13, 3.14
- PyArrow: >= 18.0.0
- PyIceberg: >= 0.7.1 (use extras `hive`, `s3fs`, `pyiceberg-core` as needed)

## Usage

### Basic Example

```python
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from iceberg_loader import LoaderConfig, load_data_to_iceberg

# 1. Connect to your catalog
catalog = load_catalog("default")

# 2. Prepare data
data = pa.Table.from_pydict({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "created_at": [1672531200000, 1672617600000, 1672704000000],
    "signup_date": ["2023-01-01", "2023-01-01", "2023-01-02"]
})

# 3. Load data
config = LoaderConfig(write_mode="append", partition_col="signup_date", schema_evolution=True)
result = load_data_to_iceberg(
    table_data=data,
    table_identifier=("my_db", "my_table"),
    catalog=catalog,
    config=config,
)

print(result)
# {'rows_loaded': 3, 'write_mode': 'append', 'partition_col': 'signup_date', ...}
```

### Idempotent Load (Replace Partition)

To safely re-load data for a specific day (avoiding duplicates):

```python
config = LoaderConfig(
    write_mode="append",
    replace_filter="signup_date == '2023-01-01'",
    partition_col="signup_date",
)

load_data_to_iceberg(table_data=data, table_identifier=("my_db", "my_table"), catalog=catalog, config=config)
```

### Upsert (Merge Into)

Perform a merge operation (update existing rows, insert new ones) based on key columns. Requires PyIceberg >= 0.7.0.

```python
config = LoaderConfig(write_mode="upsert", join_cols=["id"])
load_data_to_iceberg(table_data=data, table_identifier=("my_db", "my_table"), catalog=catalog, config=config)
```

### Batch Loading
For large datasets, use `load_batches_to_iceberg` with an iterator of RecordBatches:

```python
from iceberg_loader import load_batches_to_iceberg

def batch_generator():
    for i in range(10):
        yield some_record_batch

result = load_batches_to_iceberg(
    batch_iterator=batch_generator(),
    table_identifier=("my_db", "large_table"),
    catalog=catalog,
    config=LoaderConfig(write_mode="append", commit_interval=100),
)
```

### Stream Loading (Arrow IPC)
Load data directly from an Apache Arrow IPC stream (e.g., from a file or network socket):

```python
from iceberg_loader import load_ipc_stream_to_iceberg

# stream_source can be a file path or a file-like object (BytesIO)
result = load_ipc_stream_to_iceberg(
    stream_source="data.arrow",
    table_identifier=("my_db", "stream_table"),
    catalog=catalog,
    config=LoaderConfig(write_mode="append"),
)
```

### Custom Settings

You can override default table properties:

```python
custom_props = {
    'write.parquet.compression-codec': 'snappy',
    'history.expire.min-snapshots-to-keep': 5,
}

config = LoaderConfig(table_properties=custom_props, write_mode="append")
load_data_to_iceberg(..., config=config)
```

### Maintenance helper

```python
from iceberg_loader import expire_snapshots

table = catalog.load_table(("db", "users"))
expire_snapshots(table, keep_last=2)
```

## API Reference

### `load_batches_to_iceberg()`

Main function for loading a stream of batches into an Iceberg table.

```python
def load_batches_to_iceberg(
    batch_iterator: Iterator[pa.RecordBatch] | pa.RecordBatchReader,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    write_mode: Literal['overwrite', 'append', 'upsert'] = 'overwrite',
    partition_col: str | None = None,
    replace_filter: str | None = None,
    schema_evolution: bool = False,
    table_properties: dict[str, Any] | None = None,
    commit_interval: int = 0,
    join_cols: list[str] | None = None,
) -> dict[str, Any]
```

#### Parameters

**`batch_iterator`** *(Iterator[pa.RecordBatch] | pa.RecordBatchReader)*
- Iterator or reader that returns PyArrow RecordBatch objects
- Can be a generator, list of batches, or `pa.RecordBatchReader`
- Enables processing large volumes of data without loading the entire dataset into memory

**`table_identifier`** *(tuple[str, str])*
- Table identifier in the format `(namespace, table_name)`
- Example: `("my_database", "my_table")`
- If the table doesn't exist, it will be created automatically

**`catalog`** *(Catalog)*
- PyIceberg catalog instance (from `pyiceberg.catalog.load_catalog()`)
- Manages metadata and connection to the table storage
- Supports Hive, REST, Glue, and other catalog types

**`write_mode`** *(Literal['overwrite', 'append', 'upsert'], default='overwrite')*
- Data write mode:
  - `'overwrite'`: First batch overwrites the table, subsequent batches are appended
  - `'append'`: All batches are appended to existing data
  - `'upsert'`: Merge operation (update/insert) based on `join_cols`
- Can be combined with `replace_filter` for idempotent writes (only for `'append'`)

**`partition_col`** *(str | None, default=None)*
- Column name (and optional transform) for table partitioning
- Used only when creating a new table
- Supported syntax:
  - `"col_name"` (Identity transform)
  - `"year(col_name)"`
  - `"month(col_name)"`
  - `"day(col_name)"`
  - `"hour(col_name)"`
  - `"bucket(N, col_name)"` (e.g., `"bucket(16, id)"`)
  - `"truncate(W, col_name)"` (e.g., `"truncate(4, name)"`)


**`replace_filter`** *(str | None, default=None)*
- SQL-like filter for idempotent writes (works only with `write_mode='append'`)
- Deletes existing rows matching the filter before the first write
- Example: `"event_date == '2023-01-01'"` or `"year == 2023 AND month == 1"`
- Used for safe partition reloading

**`join_cols`** *(list[str] | None, default=None)*
- List of column names to use as keys for upsert operations
- Required when `write_mode='upsert'`
- Example: `["id"]` or `["user_id", "date"]`

**`schema_evolution`** *(bool, default=False)*
- Enables automatic table schema evolution
- When `True`: new columns from incoming data are automatically added to the table
- When `False`: incoming data must match the existing schema
- When schema changes, buffer is flushed and committed

**`table_properties`** *(dict[str, Any] | None, default=None)*
- Additional Iceberg table properties
- Applied only when creating a new table
- Examples:
  ```python
  {
      'write.parquet.compression-codec': 'zstd',
      'write.metadata.compression-codec': 'gzip',
      'history.expire.min-snapshots-to-keep': 10
  }
  ```

**`commit_interval`** *(int, default=0)*
- Transaction commit frequency (number of batches)
- `0`: all batches are written in a single transaction (default)
- `> 0`: commit is performed every N batches
- Useful for long data streams to manage memory and create checkpoints
- Example: `commit_interval=100` will create a snapshot every 100 batches

#### Return Value

Dictionary with loading results:
```python
{
    'rows_loaded': int,           # Total number of rows loaded
    'batches_processed': int,     # Number of batches processed
    'write_mode': str,            # Write mode used
    'partition_col': str | None,  # Partitioning column
    'schema_evolution': bool,     # Whether schema_evolution was enabled
    'commit_interval': int        # Commit interval used
}
```

#### Usage Examples

**Basic stream loading:**
```python
def generate_batches():
    for i in range(100):
        data = {"id": [i], "value": [f"row_{i}"]}
        yield pa.RecordBatch.from_pydict(data)

result = load_batches_to_iceberg(
    batch_iterator=generate_batches(),
    table_identifier=("db", "table"),
    catalog=catalog,
    write_mode="append"
)
```

**With commits for memory management:**
```python
result = load_batches_to_iceberg(
    batch_iterator=large_batch_stream,
    table_identifier=("db", "large_table"),
    catalog=catalog,
    commit_interval=50,  # Commit every 50 batches
    schema_evolution=True
)
```

**Idempotent partition loading:**
```python
result = load_batches_to_iceberg(
    batch_iterator=daily_batches,
    table_identifier=("db", "events"),
    catalog=catalog,
    write_mode="append",
    replace_filter="event_date == '2023-12-09'",
    partition_col="event_date"
)
```

## Development

This project uses [Hatch](https://hatch.pypa.io/) as build backend. For local workflows prefer [uv](https://docs.astral.sh/uv/).

### Run Tests

```bash
uv run python -m pytest
```

### Linting

```bash
uv run ruff check
uv run ruff format --check
uv run mypy
```

### Release

```bash
# Build artifacts
hatch build

# (Optional) check README/metadata
twine check dist/*

# Upload to TestPyPI
twine upload --repository testpypi dist/*

# Upload to PyPI
twine upload dist/*
```

## Status / TestPyPI

Work in progress. Test build:
```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple "iceberg-loader[all]==0.0.1"
```
Package page: https://test.pypi.org/project/iceberg-loader/0.0.1/

GitHub Actions release runs on tag `v*` (see `.github/workflows/release.yml`). Required secrets:
- `PYPI_API_TOKEN` for PyPI
- `TEST_PYPI_API_TOKEN` (optional) for TestPyPI

### Examples

See `examples/` directory for runnable scripts (requires local docker environment):

```bash
cd examples && docker-compose up -d
hatch run python examples/load_example.py
hatch run python examples/advanced_scenarios.py
hatch run python examples/load_complex_json.py
hatch run python examples/load_stream.py
hatch run python examples/load_with_commits.py
hatch run python examples/load_from_api.py
```

## Documentation

Rendered docs (MkDocs) live in `docs/` (serve locally with `mkdocs serve`). The homepage mirrors the README for quick onboarding.

## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, coding style, and PR guidelines. Quick checks:

```bash
hatch run lint
hatch run test
```

## Contributors

Thanks to all contributors who have helped make this project better!

<a href="https://github.com/IvanMatveev/iceberg-loader/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=IvanMatveev/iceberg-loader" />
</a>

Made with [contrib.rocks](https://contrib.rocks).

## License

`iceberg-loader` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
