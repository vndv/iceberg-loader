<p align="center">
  <img src="https://raw.githubusercontent.com/vndvtech/iceberg-loader/main/logo.png" alt="iceberg-loader" width="600">
</p>

# iceberg-loader

[![PyPI - Version](https://img.shields.io/pypi/v/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![Total Downloads](https://img.shields.io/pepy/dt/iceberg-loader.svg)](https://pepy.tech/projects/iceberg-loader)
[![CI](https://github.com/vndvtech/iceberg-loader/actions/workflows/ci.yml/badge.svg)](https://github.com/vndvtech/iceberg-loader/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

📚 [Documentation](https://vndvtech.github.io/iceberg-loader/)

iceberg-loader wraps [PyIceberg](https://py.iceberg.apache.org/) to load data into Apache Iceberg tables. It uses PyArrow and handles mixed JSON fields, schema evolution, idempotent replacement, upserts, batching, and streaming.


> **Status:** Actively developed and under testing.
> Tested against Hive Metastore and REST Catalog (Tabular, Polaris, self-hosted).

## Why iceberg-loader?

- **Mixed JSON fields:** converts dict and list values to strings before writing, including fields with mixed types.
- **Schema evolution:** adds columns when enabled and preserves field IDs.
- **Write modes:** append, overwrite, idempotent replacement via `replace_filter`, and upsert.
- **Streaming:** commit intervals, batches, and IPC streams.
- **Configuration:** all options live in one validated `LoaderConfig`; an `IcebergLoader` can hold a default config, and a per-call config replaces it.

## Benchmark

On a 1 GiB NYC 311 snapshot written through Polaris and MinIO, iceberg-loader took a median 3.7 s and 1.36 GiB peak RSS versus 11.4 s and 2.26 GiB for the [dlt](https://github.com/dlt-hub/dlt) Iceberg destination. Methodology and raw data: [iceberg-loader-benchmark](https://github.com/vndv/iceberg-loader-benchmark).

## Install

```bash
pip install "iceberg-loader[all]"
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add "iceberg-loader[all]"
```

| Extra | Description |
|-------|-------------|
| `hive` | Hive Metastore support |
| `s3` | S3 filesystem support |
| `rest` | REST Catalog support (Tabular, Polaris, self-hosted) |
| `all` | All extras |

Requires Python 3.10+, PyArrow >= 18, PyIceberg >= 0.10.

## Quickstart

### Hive Metastore

```python
from pyiceberg.catalog import load_catalog
from iceberg_loader import LoaderConfig, load_data_to_iceberg
from iceberg_loader.utils.arrow import create_arrow_table_from_data

catalog = load_catalog("default")
table_id = ("default", "comparison_complex_json")

data = [
    {"id": 1, "complex_field": {"a": 1, "b": "nested"}, "signup_date": "2023-01-01"},
    {"id": 2, "complex_field": {"a": 2, "b": "another", "c": [1, 2]}, "signup_date": "2023-01-02"},
    {"id": 3, "complex_field": [1, 2, 3], "signup_date": "2023-01-02"},
]

arrow_table = create_arrow_table_from_data(data)

config = LoaderConfig(write_mode="append", partition_col="day(signup_date)", schema_evolution=True)
load_data_to_iceberg(arrow_table, table_id, catalog, config=config)
```

### REST Catalog

```python
from iceberg_loader import LoaderConfig, get_rest_catalog, load_data_to_iceberg
from iceberg_loader.utils.arrow import create_arrow_table_from_data

catalog = get_rest_catalog(
    uri="https://api.tabular.io/ws",
    warehouse="s3://my-bucket/warehouse/",
    credential="your-oauth2-credential",
)
table_id = ("default", "my_table")

data = [
    {"id": 1, "name": "Alice", "signup_date": "2023-01-01"},
    {"id": 2, "name": "Bob", "signup_date": "2023-01-02"},
]

arrow_table = create_arrow_table_from_data(data)

config = LoaderConfig(write_mode="append", partition_col="day(signup_date)", schema_evolution=True)
load_data_to_iceberg(arrow_table, table_id, catalog, config=config)
```

Or use `pyiceberg.yaml` to configure your REST catalog and load it by name:

```yaml
# ~/.pyiceberg.yaml (see examples/pyiceberg.yaml.sample for a full annotated example)
catalog:
  my-rest-catalog:
    type: rest
    uri: https://your-catalog-server.com/api
    warehouse: s3://my-bucket/warehouse/
    credential: your-oauth2-credential
    s3.endpoint: https://s3.amazonaws.com
    s3.region: us-east-1
```

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("my-rest-catalog")
# ... rest is identical to above
```

> **The default `write_mode` is `'overwrite'`.** A loader call without `config`, or with a `LoaderConfig` that does not set `write_mode`, replaces all data in the table. Set `write_mode="append"` explicitly unless you mean to overwrite.

## Which function to use?

| Function                     | Use when...                                                  | Input Format                      |
|------------------------------|--------------------------------------------------------------|-----------------------------------|
| `load_data_to_iceberg`       | You have a single `pa.Table` in memory.                      | `pyarrow.Table`                   |
| `load_batches_to_iceberg`    | You have a generator/iterator of batches (memory efficient). | Iterator of `pyarrow.RecordBatch` |
| `load_ipc_stream_to_iceberg` | You are reading from an Arrow IPC stream file/socket.        | File-like object or path          |

## Documentation

The [documentation site](https://vndvtech.github.io/iceberg-loader/) covers data preparation helpers, idempotent loads, upserts, batching, table properties, logging, the full `LoaderConfig` and API reference, and a **Behavior & gotchas** section (commit atomicity, dropped columns, partitioning limits) worth reading before production use. Versioning policy and the public API are described there too; changes are listed in [RELEASE.md](RELEASE.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, coding style, PR guidelines, and the release checklist.

```bash
uv run ruff check . && uv run ruff format --check . && uv run ty check
uv run pytest
```

## Contributors

<a href="https://github.com/vndvtech/iceberg-loader/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=vndvtech/iceberg-loader" />
</a>

Made with [contrib.rocks](https://contrib.rocks).

## License

`iceberg-loader` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
