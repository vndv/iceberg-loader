# iceberg-loader

A convenience wrapper around [PyIceberg](https://py.iceberg.apache.org/) that simplifies data loading into Apache Iceberg tables. PyArrow-first, handles messy JSON, schema evolution, idempotent replace, upsert, batching, and streaming out of the box.

> **Status:** Actively developed and under testing. PRs are welcome!
> Currently tested against Hive Metastore; REST Catalog support is planned.

## Features

- **Arrow-first:** `pa.Table`, `RecordBatch`, IPC.
- **Messy JSON friendly:** dict/list/mixed → JSON strings.
- **Schema evolution** (opt-in).
- **Idempotent replace** (`replace_filter`) and **upsert**.
- **Commit interval** for long streams.
- **Maintenance helpers** (expire snapshots).

## Install

```bash
pip install "iceberg-loader[all]"
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv pip install "iceberg-loader[all]"
```

### Extras

| Extra | Description |
|-------|-------------|
| `hive` | Hive Metastore support |
| `s3fs` | S3 filesystem support |
| `pyiceberg-core` | PyIceberg core |
| `all` | All extras |

## Compatibility

- Python: 3.10, 3.11, 3.12, 3.13, 3.14
- PyArrow: >= 18.0.0
- PyIceberg: >= 0.7.1

---

## Quickstart

```python
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from iceberg_loader import LoaderConfig, load_data_to_iceberg

catalog = load_catalog("default")
data = pa.Table.from_pydict({"id": [1, 2], "signup_date": ["2023-01-01", "2023-01-02"]})

config = LoaderConfig(write_mode="append", partition_col="day(signup_date)", schema_evolution=True)
load_data_to_iceberg(data, ("db", "users"), catalog, config=config)
```

---

## Usage

### Basic Example

```python
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from iceberg_loader import LoaderConfig, load_data_to_iceberg

catalog = load_catalog("default")

data = pa.Table.from_pydict({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "created_at": [1672531200000, 1672617600000, 1672704000000],
    "signup_date": ["2023-01-01", "2023-01-01", "2023-01-02"]
})

config = LoaderConfig(write_mode="append", partition_col="day(signup_date)", schema_evolution=True)
result = load_data_to_iceberg(
    table_data=data,
    table_identifier=("my_db", "my_table"),
    catalog=catalog,
    config=config,
)

print(result)
# {'rows_loaded': 3, 'write_mode': 'append', 'partition_col': 'day(signup_date)', ...}
```

### Idempotent Load (Replace Partition)

Safely re-load data for a specific day (avoiding duplicates):

```python
config = LoaderConfig(
    write_mode="append",
    replace_filter="signup_date == '2023-01-01'",
    partition_col="day(signup_date)",
)

load_data_to_iceberg(table_data=data, table_identifier=("my_db", "my_table"), catalog=catalog, config=config)
```

### Upsert (Merge Into)

Merge operation (update existing rows, insert new ones) based on key columns. Requires PyIceberg >= 0.7.0.

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

Load data directly from an Apache Arrow IPC stream:

```python
from iceberg_loader import load_ipc_stream_to_iceberg

result = load_ipc_stream_to_iceberg(
    stream_source="data.arrow",
    table_identifier=("my_db", "stream_table"),
    catalog=catalog,
    config=LoaderConfig(write_mode="append"),
)
```

### Custom Settings

Override default table properties:

```python
custom_props = {
    'write.parquet.compression-codec': 'snappy',
    'history.expire.min-snapshots-to-keep': 5,
}

config = LoaderConfig(table_properties=custom_props, write_mode="append")
load_data_to_iceberg(..., config=config)
```

### Override Default Table Properties Globally

Built-in defaults live in `iceberg_loader.core.config.TABLE_PROPERTIES` (format version, Parquet compression, commit retries). To tweak them, copy the dictionary, override keys, and pass into `LoaderConfig`:

```python
from pyiceberg.catalog import load_catalog
from iceberg_loader import LoaderConfig, load_data_to_iceberg
from iceberg_loader.core.config import TABLE_PROPERTIES

catalog = load_catalog("default")

custom_properties = {**TABLE_PROPERTIES, "write.parquet.compression-codec": "gzip"}

config = LoaderConfig(
    write_mode="append",
    table_properties=custom_properties,
)

load_data_to_iceberg(table_data, ("default", "events"), catalog, config=config)
```

### Maintenance Helper

```python
from iceberg_loader import expire_snapshots

table = catalog.load_table(("db", "users"))
expire_snapshots(table, keep_last=2)
```

### Adding Load Timestamp

You can automatically add a timestamp column (e.g. `_load_dttm`) to every row to track when it was loaded. This is useful for ETL audit trails or partitioning by load time.

```python
from datetime import datetime

# Will add column '_load_dttm' with current time and partition by hour
config = LoaderConfig(
    write_mode="append",
    load_timestamp=datetime.now(),
    partition_col="hour(_load_dttm)",
)

# You can also customize the column name and day-transform it
config_custom = LoaderConfig(
    write_mode="append",
    load_timestamp=datetime(2025, 1, 1),
    load_ts_col="etl_ts",
    partition_col="day(etl_ts)",
)
```

---

## LoaderConfig Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `write_mode` | `'append'` \| `'overwrite'` \| `'upsert'` | `'overwrite'` | Data write mode |
| `partition_col` | `str \| None` | `None` | Partition column or transform (e.g., `month(ts)`, `bucket(16,id)`); prefer day/hour on timestamps |
| `replace_filter` | `str \| None` | `None` | SQL-style filter for idempotent loads |
| `schema_evolution` | `bool` | `False` | Auto-add new columns |
| `commit_interval` | `int` | `0` | Commit every N batches (0 = single transaction) |
| `join_cols` | `list[str] \| None` | `None` | Merge keys for upsert |
| `table_properties` | `dict \| None` | `None` | Custom Iceberg table properties |
| `load_timestamp` | `datetime \| None` | `None` | If set, adds `_load_dttm` column with this value |
| `load_ts_col` | `str` | `'_load_dttm'` | Name of the load timestamp column |

---

## API Reference

### `load_batches_to_iceberg()`

Main function for loading a stream of batches into an Iceberg table.

```python
def load_batches_to_iceberg(
    batch_iterator: Iterator[pa.RecordBatch] | pa.RecordBatchReader,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    config: LoaderConfig | None = None,
) -> dict[str, Any]
```

All load parameters (write_mode, partition_col, replace_filter, join_cols, schema_evolution, commit_interval, table_properties) are passed **only** via `LoaderConfig`. See the table in **LoaderConfig Reference** above.

#### Return Value

Dictionary with loading results:
```python
{
    'rows_loaded': int,
    'batches_processed': int,
    'write_mode': str,
    'partition_col': str,
    'table_location': str,
    'snapshot_id': int | str,
    'new_table_created': bool,
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
    config=LoaderConfig(write_mode="append")
)
```

**With commits for memory management:**
```python
result = load_batches_to_iceberg(
    batch_iterator=large_batch_stream,
    table_identifier=("db", "large_table"),
    catalog=catalog,
    config=LoaderConfig(
        commit_interval=50,
        schema_evolution=True
    )
)
```

**Idempotent partition loading:**
```python
result = load_batches_to_iceberg(
    batch_iterator=daily_batches,
    table_identifier=("db", "events"),
    catalog=catalog,
    config=LoaderConfig(
        write_mode="append",
        replace_filter="event_date == '2023-12-09'",
        partition_col="day(event_date)"
    )
)
```

---

## Examples

See the [Examples](examples.md) page for runnable demos covering streaming, upsert, schema evolution, messy JSON, and more.

---

## How we version

- Semantic Versioning from `0.1.x`: MINOR = new compatible features, PATCH = fixes, MAJOR = breaking changes.
- Public API: `LoaderConfig`, `load_data_to_iceberg`, `load_batches_to_iceberg`, `load_ipc_stream_to_iceberg`; other modules are internal.
- Prefer partition transforms for timestamps (`day(ts)`, `hour(ts)`, `day(_load_dttm)`) to avoid unbounded partition counts.
- LoaderConfig validates partition expressions and forbids unsafe mixes (e.g., `replace_filter` with `upsert`, identity partition on `_load_dttm`).

## Release checklist

- Align versions in `pyproject.toml` and `src/iceberg_loader/__about__.py`.
- Update `RELEASE.md` with highlights/breaking changes.
- Run `uv lock --locked` and commit `uv.lock` if it changes.
- Run lint (`uv run ruff check .`), types (`uv run mypy src/iceberg_loader tests`), and tests (`uv run python -m pytest`).
- Tag and push (`git tag -a vX.Y.Z -m "Release X.Y.Z"`), let CI publish.

---

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
hatch build
twine check dist/*
twine upload --repository testpypi dist/*
twine upload dist/*
```

---

## License

`iceberg-loader` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
