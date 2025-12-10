# iceberg-loader

Utilities for loading data into Apache Iceberg tables with PyArrow. Focused on messy JSON handling, schema evolution, idempotent/upsert writes, and streaming.

## Why iceberg-loader?
- Messy JSON friendly: dict/list/mixed fields are auto-serialized to JSON strings.
- Schema evolution: optional auto-union to add new columns on the fly.
- Idempotent writes and upserts: overwrite partitions via `replace_filter` or merge by keys with `write_mode="upsert"` + `join_cols`.
- Arrow-first: accepts `pa.Table`, `RecordBatch` iterators, and Arrow IPC streams.
- Partition transforms: strings like `month(ts)`, `year(ts)`, `bucket(16,id)`, `truncate(4,col)` parsed automatically.
- Memory-aware: `commit_interval` batches commits to avoid huge transactions.

## Install
```bash
pip install "iceberg-loader>=0.0.4"
# Extras
pip install "iceberg-loader[hive]"
pip install "iceberg-loader[s3]"
pip install "iceberg-loader[all]"
```

## Quickstart
```python
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from iceberg_loader import load_data_to_iceberg, LoaderConfig

catalog = load_catalog("default")
table = pa.Table.from_pydict(
    {"id": [1, 2], "name": ["Alice", "Bob"], "signup_date": ["2023-01-01", "2023-01-02"]}
)

config = LoaderConfig(
    write_mode="append",
    partition_col="signup_date",
    schema_evolution=True,
)

result = load_data_to_iceberg(
    table_data=table,
    table_identifier=("db", "users"),
    catalog=catalog,
    config=config,
)
print(result)
```

## LoaderConfig cheatsheet
- `write_mode`: `append` | `overwrite` | `upsert`
- `partition_col`: column or transform string (`month(ts)`, `bucket(16,id)`, etc.)
- `replace_filter`: SQL-style filter for overwrite/idempotent loads
- `schema_evolution`: auto add columns when needed
- `commit_interval`: flush every N batches (streaming/large loads)
- `join_cols`: required for `upsert` (merge keys)
- `table_properties`: overrides default Iceberg table props

## Common patterns
- Append: `write_mode="append"`
- Overwrite partition: `write_mode="overwrite"`, `replace_filter="ts='2023-01-01'"`, `partition_col="ts"`
- Upsert: `write_mode="upsert"`, `join_cols=["id"]`
- Streaming IPC: use `load_ipc_stream_to_iceberg` with `commit_interval` to avoid huge transactions

## Examples
- Basic load: `examples/load_example.py`
- Advanced scenarios (schema evolution, partitions, overwrite): `examples/advanced_scenarios.py`
- Complex JSON handling: `examples/load_complex_json.py`
- IPC streaming: `examples/optional/load_stream.py`
- REST API ingestion: `examples/optional/load_from_api.py`
- Upsert demo: `examples/load_upsert.py`
- Commit interval demo: `examples/load_with_commits.py`

Run with local MinIO + Hive:
```bash
cd examples
docker-compose up -d
hatch run python examples/load_example.py
```

## Development
```bash
hatch run lint
hatch run types:check
hatch run test
```

## Maintenance
```python
from iceberg_loader import expire_snapshots

table = catalog.load_table(("db", "users"))
expire_snapshots(table, keep_last=2)
```

## License
MIT

