# iceberg-loader

[![PyPI - Version](https://img.shields.io/pypi/v/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![Coverage](https://img.shields.io/badge/coverage-88%25-brightgreen)](coverage.xml)
[![CI](https://github.com/vndvtech/iceberg-loader/actions/workflows/ci.yml/badge.svg)](https://github.com/vndvtech/iceberg-loader/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

📚 [Documentation](https://vndvtech.github.io/iceberg-loader/)

A convenience wrapper around [PyIceberg](https://py.iceberg.apache.org/) that simplifies data loading into Apache Iceberg tables. PyArrow-first, handles messy JSON, schema evolution, idempotent replace, upsert, batching, and streaming out of the box.


> **Status:** Actively developed and under testing. PRs are welcome!
> Currently tested against Hive Metastore; REST Catalog support is planned.

## Why iceberg-loader?

- **Messy JSON friendly:** auto-serializes dict/list/mixed fields to strings so writes don't fail.
- **Schema evolution:** add columns on the fly (opt-in), preserves field IDs.
- **Safe writes:** append/overwrite, idempotent replace via `replace_filter`, upsert.
- **Stream friendly:** commit intervals, batches, IPC streams.
- **Single config:** `LoaderConfig` sets defaults; override per-call if needed.

## Install

```bash
pip install "iceberg-loader[all]"
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add "iceberg-loader[all]"
```

## Quickstart

```python
from iceberg_loader import LoaderConfig, load_data_to_iceberg
from iceberg_loader.utils.arrow import create_arrow_table_from_data

catalog = load_catalog("default")
table_id = ("default", "comparison_complex_json")

data = [
    {"id": 1, "complex_field": {"a": 1, "b": "nested"}},
    {"id": 2, "complex_field": {"a": 2, "b": "another", "c": [1, 2]}},
    {"id": 3, "complex_field": [1, 2, 3]},
]

arrow_table = create_arrow_table_from_data(data)

config = LoaderConfig(write_mode="append", partition_col="signup_date", schema_evolution=True)
load_data_to_iceberg(arrow_table, table_id, catalog, config=config)
```

## Which function to use?

| Function                     | Use when...                                                  | Input Format                      |
|------------------------------|--------------------------------------------------------------|-----------------------------------|
| `load_data_to_iceberg`       | You have a single `pa.Table` in memory.                      | `pyarrow.Table`                   |
| `load_batches_to_iceberg`    | You have a generator/iterator of batches (memory efficient). | Iterator of `pyarrow.RecordBatch` |
| `load_ipc_stream_to_iceberg` | You are reading from an Arrow IPC stream file/socket.        | File-like object or path          |

## Preparing Data

Use helpers to convert Python dictionaries to Arrow format (handling messy types automatically):

```python
from iceberg_loader.utils.arrow import create_arrow_table_from_data, create_record_batches_from_dicts

# 1. Convert list of dicts -> pa.Table
arrow_table = create_arrow_table_from_data(data_list)

# 2. Convert iterator of dicts -> Iterator[pa.RecordBatch]
batches = create_record_batches_from_dicts(data_generator(), batch_size=10000)
```

Alternatively, use standard PyArrow conversion: `pa.Table.from_pylist(data)`.


## Contributing

We welcome contributions! See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, coding style, and PR guidelines.

```bash
hatch run lint
hatch run test
```

## Contributors

Thanks to all contributors who have helped make this project better!

<a href="https://github.com/vndvtech/iceberg-loader/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=vndvtech/iceberg-loader" />
</a>

Made with [contrib.rocks](https://contrib.rocks).

## License

`iceberg-loader` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
