# Examples

Runnable examples demonstrating various features of `iceberg-loader`. All examples are located in the [`examples/`](https://github.com/vndvtech/iceberg-loader/tree/main/examples) directory.

## Prerequisites

You need a running Iceberg catalog (e.g., Hive Metastore) and MinIO/S3. Use the bundled `docker-compose.yml` to start a local stack (run from repo root):

```bash
cd examples
docker-compose up -d
```

Then run examples from the same `examples/` directory (see commands below). With `uv` you can prefix any command as `uv run python <script.py>`.

## Core Examples

| Example | Description |
|---------|-------------|
| [`load_with_commits.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/load_with_commits.py) | Commit interval for long streams |
| [`load_upsert.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/load_upsert.py) | Upsert (merge) by key columns |
| [`advanced_scenarios.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/advanced_scenarios.py) | Schema evolution, custom types, partitioning |
| [`load_complex_json.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/load_complex_json.py) | Messy JSON handling |
| [`compare_complex_json_fail.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/compare_complex_json_fail.py) | PyArrow fails on mixed types, iceberg-loader succeeds |

## Other Examples

| Example | Description |
|---------|-------------|
| [`load_stream.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/load_stream.py) | Arrow IPC stream loading |
| [`load_from_api.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/load_from_api.py) | Simulated REST API ingestion |
| [`maintenance_example.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/maintenance_example.py) | Snapshot expiration |

## Running

Run from the `examples/` directory:

```bash
cd examples

# Core
python load_with_commits.py
python load_upsert.py
python advanced_scenarios.py
python load_complex_json.py

# Other
python load_stream.py
python load_from_api.py
python maintenance_example.py
```

With [uv](https://docs.astral.sh/uv/):

```bash
uv run python load_with_commits.py
uv run python load_upsert.py
uv run python advanced_scenarios.py
uv run python load_complex_json.py
uv run python load_stream.py
uv run python load_from_api.py
uv run python maintenance_example.py
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

### Dynamic Configuration (Multi-table Load)

When loading multiple tables in a loop, you can dynamically switch `LoaderConfig` for each table:

```python
# Define configurations
config_overwrite = LoaderConfig(write_mode='overwrite', schema_evolution=True)
config_upsert = LoaderConfig(write_mode='upsert', join_cols=['id'], schema_evolution=True)

# Map endpoints/tables to specific configs
endpoint_configs = {
    'customers': config_overwrite,
    'orders': config_upsert,
}

for endpoint in endpoints:
    # Use specific config or default to append
    current_config = endpoint_configs.get(endpoint, LoaderConfig(write_mode='append'))
    
    load_data_to_iceberg(
        table_data=data,
        table_identifier=('default', endpoint),
        catalog=catalog,
        config=current_config
    )
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
