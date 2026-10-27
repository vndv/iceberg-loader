# Examples

The scripts in [`examples/`](https://github.com/vndv/iceberg-loader/tree/main/examples) cover loading, catalog setup, and maintenance.

## Prerequisites

You need a running Iceberg catalog and MinIO/S3. The bundled `docker-compose.yml` starts a local stack (run from the `examples/` directory):

```bash
cd examples
docker compose up -d
```

The bundled stack includes:

- **MinIO** (S3) at `http://localhost:9000` (console `http://localhost:9001`, `minio`/`minio123`)
- **Hive Metastore** at `thrift://localhost:9083`
- **Trino** at `http://localhost:8080`
- **Apache Polaris** REST Catalog at `http://localhost:8181/api/catalog` (`root`/`root`)

Then run examples from the same `examples/` directory (see commands below). With `uv` you can prefix any command as `uv run python <script.py>`.

## Core Examples

| Example | Description |
|---------|-------------|
| [`load_with_commits.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/load_with_commits.py) | Commit interval for long streams |
| [`load_upsert.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/load_upsert.py) | Upsert (merge) by key columns |
| [`advanced_scenarios.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/advanced_scenarios.py) | Schema evolution, custom types, partitioning |
| [`load_complex_json.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/load_complex_json.py) | Messy JSON handling |
| [`compare_complex_json_fail.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/compare_complex_json_fail.py) | PyArrow fails on mixed types, iceberg-loader succeeds |

## Other Examples

| Example | Description |
|---------|-------------|
| [`load_stream.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/load_stream.py) | Arrow IPC stream loading |
| [`load_from_api.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/load_from_api.py) | Simulated REST API ingestion |
| [`maintenance_example.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/maintenance_example.py) | Snapshot expiration |
| [`rest_catalog_example.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/rest_catalog_example.py) | REST Catalog setup (Polaris/Tabular) with `get_rest_catalog()` |
| [`load_timestamp_partitioning.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/load_timestamp_partitioning.py) | `_load_dttm` load timestamp + time-transform partitioning |
| [`load_from_duckdb.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/load_from_duckdb.py) | DuckDB as source and SQL transform engine, loaded from an Arrow result |
| [`load_from_polars.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/load_from_polars.py) | Polars `DataFrame` transform, loaded via `to_arrow()` |
| [`load_pyarrow_dataset.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/load_pyarrow_dataset.py) | PyArrow `Dataset` streaming with `commit_interval` |
| [`load_observability.py`](https://github.com/vndv/iceberg-loader/blob/main/examples/load_observability.py) | Load timings and per-column cast counters |

## Running

Run from the `examples/` directory:

```bash
cd examples

# Core
python load_with_commits.py
python load_upsert.py
python advanced_scenarios.py
python load_complex_json.py
python compare_complex_json_fail.py

# Other
python load_stream.py
python load_from_api.py
python maintenance_example.py
python rest_catalog_example.py
python load_timestamp_partitioning.py
python load_pyarrow_dataset.py
python load_from_duckdb.py   # needs duckdb
python load_from_polars.py   # needs polars
python load_observability.py
```

With [uv](https://docs.astral.sh/uv/):

```bash
uv run python load_with_commits.py
uv run python load_upsert.py
uv run python advanced_scenarios.py
uv run python load_complex_json.py
uv run python compare_complex_json_fail.py
uv run python load_stream.py
uv run python load_from_api.py
uv run python maintenance_example.py
uv run python rest_catalog_example.py
uv run python load_timestamp_partitioning.py
uv run python load_pyarrow_dataset.py
uv run --with duckdb python load_from_duckdb.py
uv run --with polars python load_from_polars.py
uv run python load_observability.py
```

---

## Example Highlights

### Loading Data in Batches

Load data from an iterator of RecordBatches:

```python
import pyarrow as pa
from iceberg_loader import LoaderConfig, load_batches_to_iceberg

def generate_batches():
    for i in range(10):
        data = {
            'id': list(range(i * 100, (i + 1) * 100)),
            'name': [f'Item_{j}' for j in range(100)],
        }
        yield pa.RecordBatch.from_pydict(data)

config = LoaderConfig(write_mode="append", schema_evolution=True)
result = load_batches_to_iceberg(
    batch_iterator=generate_batches(),
    table_identifier=("db", "items"),
    catalog=catalog,
    config=config,
)
```

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

Choose a `LoaderConfig` for each table in a loop:

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

For mixed or nested values that PyArrow cannot convert directly, `iceberg-loader` serializes dicts and lists as JSON strings:

```python
from iceberg_loader import LoaderConfig, load_data_to_iceberg
from iceberg_loader.utils.arrow import create_arrow_table_from_data

data = [
    {"id": 1, "complex_field": {"a": 1, "b": "nested"}},
    {"id": 2, "complex_field": {"a": 2, "b": "another", "c": [1, 2]}},
    {"id": 3, "complex_field": [1, 2, 3]},
]

arrow_table = create_arrow_table_from_data(data)

config = LoaderConfig(write_mode="append")
load_data_to_iceberg(arrow_table, ("db", "events"), catalog, config=config)
```

### Schema Evolution

Set `schema_evolution=True` to add columns found in incoming data:

```python
config = LoaderConfig(write_mode="append", schema_evolution=True)
load_data_to_iceberg(data_with_new_columns, ("db", "table"), catalog, config=config)
```

### Partitioning

Create partitioned tables with transform expressions:

```python
from datetime import datetime

config = LoaderConfig(
    write_mode="append",
    partition_col="month(event_date)",  # or day(), year(), bucket(16, id), etc.
)
load_data_to_iceberg(data, ("db", "events"), catalog, config=config)

ingestion_config = LoaderConfig(
    write_mode="append",
    load_timestamp=datetime.now(),
    partition_col="hour(_load_dttm)",
)
```
