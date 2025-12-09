# iceberg-loader

Utilities for loading data into Iceberg tables using PyArrow. Handles messy JSON, schema evolution, idempotent writes, and streaming.

## Why iceberg-loader?
- **Sanitize messy JSON**: dicts/lists and mixed types are auto-serialized to JSON strings.
- **Schema evolution**: optional auto-union to add new columns.
- **Idempotent writes**: safely replace partitions with `replace_filter`.
- **Arrow-first**: works with `pa.Table`, `RecordBatch`, and Arrow IPC streams.

## Install
```bash
pip install "iceberg-loader>=0.0.1"
```

Hive/S3 extras:
```bash
pip install "iceberg-loader[hive]"
pip install "iceberg-loader[s3]"
pip install "iceberg-loader[all]"
```

## Quickstart
```python
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from iceberg_loader import load_data_to_iceberg

catalog = load_catalog("default")
table = pa.Table.from_pydict({"id": [1, 2], "name": ["Alice", "Bob"], "signup_date": ["2023-01-01", "2023-01-02"]})

result = load_data_to_iceberg(
    table_data=table,
    table_identifier=("db", "users"),
    catalog=catalog,
    write_mode="append",
    partition_col="signup_date",
    schema_evolution=True,
)
print(result)
```

## Examples
- Basic load: `examples/load_example.py`
- Advanced scenarios: `examples/advanced_scenarios.py`
- Complex JSON: `examples/load_complex_json.py`
- Arrow IPC streaming: `examples/load_stream.py`
- REST API ingestion: `examples/load_from_api.py`

Run with local infra (MinIO + Hive):
```bash
cd examples
docker-compose up -d
hatch run python examples/load_example.py
```

## Development
```bash
hatch run lint
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

