# Examples

Runnable examples demonstrating various features of `iceberg-loader`. All examples are located in the [`examples/`](https://github.com/IvanMatveev/iceberg-loader/tree/main/examples) directory.

## Prerequisites

You need a running Iceberg catalog (e.g., Hive Metastore) and MinIO/S3. A `docker-compose.yml` is provided to spin up a local environment:

```bash
cd examples
docker-compose up -d
```

## Core Examples

| Example | Description |
|---------|-------------|
| [`load_with_commits.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/load_with_commits.py) | Commit interval for long streams |
| [`load_upsert.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/load_upsert.py) | Upsert (merge) by key columns |
| [`advanced_scenarios.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/advanced_scenarios.py) | Schema evolution, custom types, partitioning |
| [`load_complex_json.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/load_complex_json.py) | Messy JSON handling |
| [`compare_complex_json_fail.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/compare_complex_json_fail.py) | PyArrow fails on mixed types, iceberg-loader succeeds |

## Optional Examples

| Example | Description |
|---------|-------------|
| [`optional/load_stream.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/optional/load_stream.py) | Arrow IPC stream loading |
| [`optional/load_from_api.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/optional/load_from_api.py) | Simulated REST API ingestion |
| [`optional/maintenance_example.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/optional/maintenance_example.py) | Snapshot expiration |

## Running

Run from the `examples/` directory:

```bash
cd examples

# Core
python load_with_commits.py
python load_upsert.py
python advanced_scenarios.py
python load_complex_json.py

# Optional
python optional/load_stream.py
python optional/load_from_api.py
python optional/maintenance_example.py
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv run python load_with_commits.py
```

---

## Example Highlights

### Commit Interval (Streaming)

Use `commit_interval` to flush batches periodically during long-running streams:

```python
from iceberg_loader import LoaderConfig, load_batches_to_iceberg

config = LoaderConfig(write_mode="append", commit_interval=100)
result = load_batches_to_iceberg(
    batch_iterator=my_batch_generator(),
    table_identifier=("db", "events"),
    catalog=catalog,
    config=config,
)
```

### Upsert (Merge)

Perform merge operations (update existing, insert new) based on key columns:

```python
config = LoaderConfig(write_mode="upsert", join_cols=["id"])
load_data_to_iceberg(data, ("db", "users"), catalog, config=config)
```

### Messy JSON

iceberg-loader auto-serializes mixed/nested types to JSON strings when PyArrow would fail:

```python
data = pa.Table.from_pydict({
    "id": [1, 2],
    "metadata": [{"key": "value"}, [1, 2, 3]],  # mixed types
})

config = LoaderConfig(write_mode="append")
load_data_to_iceberg(data, ("db", "events"), catalog, config=config)
```

### Schema Evolution

Automatically add new columns when data schema changes:

```python
config = LoaderConfig(write_mode="append", schema_evolution=True)
load_data_to_iceberg(data_with_new_columns, ("db", "table"), catalog, config=config)
```

### Partitioning

Create partitioned tables with transform expressions:

```python
config = LoaderConfig(
    write_mode="append",
    partition_col="month(event_date)",  # or day(), year(), bucket(16, id), etc.
)
load_data_to_iceberg(data, ("db", "events"), catalog, config=config)
```

