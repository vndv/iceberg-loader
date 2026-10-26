# Examples

The scripts in [`examples/`](https://github.com/vndvtech/iceberg-loader/tree/main/examples) cover loading, catalog setup, and maintenance. They run against a local Docker stack (MinIO, Hive Metastore, Trino, Apache Polaris).

For prerequisites, the stack's endpoints and credentials, environment variables for a remote catalog, and run commands, see [`examples/README.md`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/README.md).

## Core examples

| Example | Description |
|---------|-------------|
| [`load_with_commits.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/load_with_commits.py) | Commit interval for long streams |
| [`load_upsert.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/load_upsert.py) | Upsert (merge) by key columns |
| [`advanced_scenarios.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/advanced_scenarios.py) | Schema evolution, custom types, partitioning |
| [`load_complex_json.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/load_complex_json.py) | Messy JSON handling |
| [`compare_complex_json_fail.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/compare_complex_json_fail.py) | PyArrow fails on mixed types, iceberg-loader succeeds |

## Other examples

| Example | Description |
|---------|-------------|
| [`load_stream.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/load_stream.py) | Arrow IPC stream loading |
| [`load_from_api.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/load_from_api.py) | Simulated REST API ingestion |
| [`maintenance_example.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/maintenance_example.py) | Snapshot expiration |
| [`rest_catalog_example.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/rest_catalog_example.py) | REST Catalog setup (Polaris/Tabular) with `get_rest_catalog()` |
| [`load_timestamp_partitioning.py`](https://github.com/vndvtech/iceberg-loader/blob/main/examples/load_timestamp_partitioning.py) | `_load_dttm` load timestamp + time-transform partitioning |
