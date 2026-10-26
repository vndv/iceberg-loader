# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Set up dev environment
uv sync

# Run all tests
uv run pytest

# Run a single test file
uv run pytest tests/test_iceberg_loader.py

# Tests use a real sqlite-backed SqlCatalog via the `sql_catalog` fixture (tests/conftest.py)

# Run tests with coverage
uv run pytest --cov=iceberg_loader --cov-report=html

# Lint + format check + type check
uv run ruff check . && uv run ruff format --check . && uv run ty check

# Auto-fix lint issues
uv run ruff check --fix .

# Format code
uv run ruff format .
```

## Architecture

The library is a thin convenience wrapper around PyIceberg for loading PyArrow data into Apache Iceberg tables. Full component map, data flow, module dependencies, and "where to make changes": `docs/ARCHITECTURE.md`.

- Public API (`src/iceberg_loader/__init__.py`): `load_data_to_iceberg`, `load_batches_to_iceberg`, `load_ipc_stream_to_iceberg`, `IcebergLoader`, `LoaderConfig`, `get_rest_catalog`, `expire_snapshots`.
- `core/loader.py` — `IcebergLoader` buffers batches up to `commit_interval`, hands each buffer to `BatchConformer` (`core/conform.py`: create/load table, load timestamp, schema evolution, casts), then writes it with a `WriteStrategy` (`core/strategies.py`).
- `core/schema.py` — `SchemaManager`: table creation, top-level schema evolution, Arrow↔Iceberg conversion. `core/config.py` — `LoaderConfig` and `TABLE_PROPERTIES`. `core/partitioning.py` — partition transform parsing.
- `catalog.py` — `get_rest_catalog()` builds a `RestCatalog` from args or env vars.
- `iceberg_loader.py` — backward-compat shim for `from iceberg_loader.iceberg_loader import ...`; it duplicates the three wrapper functions, so keep it in sync when their signatures change.
- `utils/arrow.py`, `utils/types.py` — dict→Arrow conversion, casts, type mappings. `services/logging.py`, `services/maintenance.py` — logger, `expire_snapshots`.

## Key Constraints

- `schema_evolution=False` by default; set to `True` to add columns automatically. Without it, unknown columns are dropped silently.
- The default `write_mode` is `'overwrite'`: a `LoaderConfig()` without `write_mode` (or `config=None`) replaces the table. Set `write_mode` explicitly in tests and examples.
- `commit_interval=0` (the default) means one commit per batch, same as `1` — not a single transaction. A load is atomic only if all batches fit in one buffer.
- All `table_properties` (not just `format-version`) apply only when the loader creates the table; existing tables keep their properties.
- `replace_filter` and `write_mode='upsert'` are mutually exclusive.
- Identity partition on the load timestamp column (`_load_dttm`) is rejected — use `day(...)` or `hour(...)`.
- Partition string columns used with time transforms (`year`/`month`/`day`/`hour`) are auto-promoted to `timestamp(us)` at table creation, so `replace_filter` on them needs a timestamp literal (`'2023-01-01T00:00:00'`).
- All public API options must be passed via `LoaderConfig`; avoid legacy positional arguments.

## Versioning & Release

Follow the release checklist in `CONTRIBUTING.md`. In short: bump the version in both `pyproject.toml` and `src/iceberg_loader/__about__.py` (must match), update `RELEASE.md`, run `uv lock` and `uv lock --locked`, then `git tag -a vX.Y.Z` — CI publishes to PyPI.
