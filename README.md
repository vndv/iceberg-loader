# iceberg-loader

A convenience wrapper around [PyIceberg](https://py.iceberg.apache.org/) that simplifies data loading into Apache Iceberg tables. PyArrow-first, handles messy JSON, schema evolution, idempotent replace, upsert, batching, and streaming out of the box.

[![PyPI - Version](https://img.shields.io/pypi/v/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/iceberg-loader.svg)](https://pypi.org/project/iceberg-loader)
[![Coverage](https://img.shields.io/badge/coverage-88%25-brightgreen)](coverage.xml)
[![CI](https://github.com/vndvtech/iceberg-loader/actions/workflows/ci.yml/badge.svg)](https://github.com/vndvtech/iceberg-loader/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

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
uv pip install "iceberg-loader[all]"
```

## Quickstart

```python
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from iceberg_loader import LoaderConfig, load_data_to_iceberg, create_arrow_table_from_data

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

## Documentation

Full usage guide, API reference, and examples: **[docs](https://vndvtech.github.io/iceberg-loader/)** or run `mkdocs serve` locally.

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
