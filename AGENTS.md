# AGENTS.md

Guidance for coding agents working in this repository. Humans: start with `README.md` and `CONTRIBUTING.md`.

`iceberg-loader` is a thin wrapper around PyIceberg that loads PyArrow data (`pa.Table`, record batches, Arrow IPC streams) into Apache Iceberg tables. Python 3.10+, managed with [uv](https://docs.astral.sh/uv/).

## Commands

```bash
uv sync                                    # set up the dev environment
uv run pytest                              # all tests (fast, no Docker)
uv run pytest tests/test_load_behavior.py  # one file
uv run ruff check . && uv run ruff format --check . && uv run ty check   # lint + format + types
uv run ruff check --fix . && uv run ruff format .                        # auto-fix
bash tools/run_examples_smoke.sh           # examples against the Docker stack in examples/ (slow)
```

A change is done when `pytest` and the lint/format/types line are green. Run the Docker smoke suite when you touch `examples/` or behavior the examples exercise.

## Layout

- `src/iceberg_loader/core/` — `loader.py` (`IcebergLoader`: buffers batches up to `commit_interval`, one commit per buffer; the result dict carries stage timings and cast counters), `conform.py` (`BatchConformer`: create/load table, load timestamp, schema evolution, casts), `strategies.py` (append / overwrite / idempotent replace / upsert), `schema.py` (`SchemaManager`), `config.py` (`LoaderConfig`, `TABLE_PROPERTIES`), `partitioning.py`, `register.py` (`register_parquet_files`: adds existing Parquet files without rewriting them), `watermark.py` (`get_high_watermark`).
- `src/iceberg_loader/utils/` — dict→Arrow helpers and casts (`arrow.py`), Arrow↔Iceberg type mappings (`types.py`).
- `src/iceberg_loader/services/` — package logger (`logging.py`), `expire_snapshots` (`maintenance.py`).
- `src/iceberg_loader/catalog.py` — `get_rest_catalog()`.
- `src/iceberg_loader/iceberg_loader.py` — backward-compat shim; it duplicates the three `load_*_to_iceberg` wrappers, so keep it in sync when their signatures change.
- Public API is exactly what `src/iceberg_loader/__init__.py` exports; everything else is internal.

Data flow, module dependencies, and a "where to make changes" table: `docs/ARCHITECTURE.md`. User-facing behavior and gotchas: `docs/index.md`.

## Conventions

- Style is enforced by ruff (`pyproject.toml`): single quotes, 120-char lines.
- Log through the package logger: `from iceberg_loader.services.logging import logger`. It has only a `NullHandler` and propagates to root, but `configure_logging()` (called by some test modules) turns propagation off, so in tests read warnings with the `loader_warnings` fixture, not bare `caplog`.
- Tests run against a real sqlite-backed catalog: the `sql_catalog` fixture in `tests/conftest.py`. Assert on table contents and snapshots, not on mocks.
- Every load option goes through `LoaderConfig`; new options are added there with validation.
- User-visible behavior changes get a line in the `Unreleased` section of `RELEASE.md`; breaking ones go under `Breaking Changes`.
- Docs (`docs/`, `README.md`) are in English and published with MkDocs from `main`; check them with `uvx --with mkdocs-material mkdocs build --strict`.

## Traps

- The default `write_mode` is `'overwrite'`: `LoaderConfig()` or `config=None` replaces the table. Set `write_mode` explicitly in tests, examples, and docs.
- `commit_interval=0` (the default) and `1` both mean one commit per batch. A load is atomic only when all batches fit in one buffer.
- All `table_properties`, including `format-version`, apply only when the loader creates the table.
- `schema_evolution=False` (the default) drops unknown columns silently, and with `commit_interval > 1` every batch in a buffer must have an identical schema (column order included) or `pa.concat_tables` raises `ArrowInvalid`. With `True`, only new top-level columns are added.
- A string column partitioned with `year`/`month`/`day`/`hour` is promoted to `timestamp(us)` at table creation, so `replace_filter` on it needs a timestamp literal (`'2023-01-01T00:00:00'`).
- `replace_filter` and `write_mode='upsert'` are mutually exclusive; upsert needs `join_cols`.
- Identity partitioning on the load timestamp column (`_load_dttm`) is rejected; use `day(...)` or `hour(...)`.
- Load results gain keys as features land (timings, cast counters); in tests assert on specific keys, not on the whole dict with `==`.
- `register_parquet_files` references files in place: the table does not own them, and re-registering the same paths is rejected unless `check_duplicate_files=False`, which duplicates rows.
- `get_high_watermark(mode='metadata')` raises instead of guessing for string/binary columns or tables with delete files; `auto` falls back to a scan.

## Releases

Follow the release checklist in `CONTRIBUTING.md`. Versions in `pyproject.toml` and `src/iceberg_loader/__about__.py` must match; pushing a `vX.Y.Z` tag publishes to PyPI through CI.
