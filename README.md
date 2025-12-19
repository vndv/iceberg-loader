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
    {"id": 1, "complex_field": {"a": 1, "b": "nested"}, "signup_date": "2023-01-01"},
    {"id": 2, "complex_field": {"a": 2, "b": "another", "c": [1, 2]}, "signup_date": "2023-01-02"},
    {"id": 3, "complex_field": [1, 2, 3], "signup_date": "2023-01-02"},
]

arrow_table = create_arrow_table_from_data(data)

config = LoaderConfig(write_mode="append", partition_col="day(signup_date)", schema_evolution=True)
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

## Public API & Stability

- Public surface: `LoaderConfig`, `load_data_to_iceberg`, `load_batches_to_iceberg`, `load_ipc_stream_to_iceberg`.
- Everything else is internal and may change without notice; always pass options via `LoaderConfig`.
- Avoid legacy positional arguments—use the `config` parameter only.
- LoaderConfig validates partition expressions and rejects unsafe combos (e.g., `replace_filter` with `upsert`, identity partition on `_load_dttm`).

## How we version

- Semantic Versioning starting at `0.1.x`: **MINOR** for compatible features, **PATCH** for fixes, **MAJOR** for breaking API changes.
- Breaking changes only happen on the public surface noted above.
- Prefer partition transforms for timestamps (`day(ts)`, `hour(ts)`), especially when using `load_timestamp`.

## Release checklist

- Bump version in `pyproject.toml` and `src/iceberg_loader/__about__.py` (they must match).
- Update `RELEASE.md` with highlights and breaking notes.
- Run `uv lock --locked` and commit `uv.lock` if it changes.
- Run `uv run ruff check .`, `uv run mypy src/iceberg_loader tests`, and `uv run python -m pytest`.
- Tag and push (`git tag -a vX.Y.Z ...`), then let CI publish.


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
