# iceberg-loader

Utilities for loading data into Iceberg tables using PyArrow. This library provides a robust way to handle data ingestion into Iceberg tables with features like schema evolution, idempotent writes, and automatic partitioning.

[![PyPI - Version](https://img.shields.io/pypi/v/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![CI](https://github.com/IvanMatveev/iceberg-loader/actions/workflows/ci.yml/badge.svg)](https://github.com/IvanMatveev/iceberg-loader/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Why iceberg-loader?
Although **PyIceberg** is the official and powerful library for interacting with Iceberg tables, it relies on **PyArrow** for data type inference and conversion. This strictness can be a bottleneck when dealing with "messy" real-world data, especially raw JSON events.

**Key limitations of using raw PyIceberg/PyArrow for ingestion:**
1.  **Complex/Nested Structures:** PyArrow's schema inference for deep nested structures can be fragile.
2.  **Inconsistent Data Types:** If a field is a `Struct` in one row and a `List` in another (common in loosely structured JSON), PyArrow will crash with a `ArrowInvalid` or `ArrowTypeError`.
3.  **Strict Schema:** Writing data often requires the input Arrow table to strictly match the Iceberg table schema, or requires complex setup for schema evolution.

**How `iceberg-loader` solves this:**
*   **Automatic Sanitization:** It detects complex nested structures (dicts, lists) or inconsistent types and automatically serializes them into valid JSON strings.
*   **Robustness:** Ensures that data loading continues even if incoming data types fluctuate, effectively treating complex fields as semi-structured data (`StringType`).
*   **Simplified Schema Evolution:** Handles the boilerplate of checking and updating the Iceberg table schema to match new incoming flat fields.

## Features

- **Arrow-first**: Works with `pa.Table`, `RecordBatch`, and Arrow IPC streams.
- **Messy JSON friendly**: Dicts/lists/mixed types are auto-serialized to JSON strings.
- **Schema evolution (opt-in)**: Union incoming schema with the table when enabled.
- **Idempotent writes**: Replace partitions safely with `replace_filter`.
- **Maintenance helpers**: Snapshot expiration utilities.

## Installation

```console
pip install "iceberg-loader>=0.0.1"
```

For Hive/S3 backends install with extras:

```console
pip install "iceberg-loader[hive]"   # HiveCatalog
pip install "iceberg-loader[s3]"     # S3Catalog
pip install "iceberg-loader[all]"    # both
```

## Usage

### Basic Example

```python
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from iceberg_loader import load_data_to_iceberg

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
result = load_data_to_iceberg(
    table_data=data,
    table_identifier=("my_db", "my_table"),
    catalog=catalog,
    write_mode="append",
    # Partitioning options:
    partition_col="signup_date",  # Partition by this column
    schema_evolution=True         # Allow adding new columns if they appear
)

print(result)
# {'rows_loaded': 3, 'write_mode': 'append', 'partition_col': 'signup_date', ...}
```

### Idempotent Load (Replace Partition)

To safely re-load data for a specific day (avoiding duplicates):

```python
load_data_to_iceberg(
    table_data=data,
    table_identifier=("my_db", "my_table"),
    catalog=catalog,
    write_mode="append", # Use append, but...
    replace_filter="signup_date == '2023-01-01'", # ...delete existing rows for this date first!
    partition_col="signup_date"
)
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
    catalog=catalog
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
    catalog=catalog
)
```

### Custom Settings

You can override default table properties:

```python
custom_props = {
    'write.parquet.compression-codec': 'snappy',
    'history.expire.min-snapshots-to-keep': 5
}

load_data_to_iceberg(
    ...,
    table_properties=custom_props
)
```

### Maintenance helper

```python
from iceberg_loader import expire_snapshots

table = catalog.load_table(("db", "users"))
expire_snapshots(table, keep_last=2)
```

## Development

This project uses [Hatch](https://hatch.pypa.io/) for management.

### Run Tests

```bash
hatch run test
```

### Linting

```bash
hatch run lint
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

Для GitHub Actions публикация срабатывает по тегу `v*` (см. `.github/workflows/release.yml`). Нужны секреты:
- `PYPI_API_TOKEN` для PyPI
- `TEST_PYPI_API_TOKEN` (опционально) для TestPyPI

### Examples

See `examples/` directory for runnable scripts (requires local docker environment):

```bash
cd examples && docker-compose up -d
hatch run python examples/load_example.py
hatch run python examples/advanced_scenarios.py
hatch run python examples/load_complex_json.py
hatch run python examples/load_stream.py
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

## License

`iceberg-loader` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
