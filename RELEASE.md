# Release 0.3.1 — 2026-10-27

## Highlights
- **Dependencies**: `pyiceberg-core` is now a base dependency (`pyiceberg[pyiceberg-core]`). A plain `pip install iceberg-loader` used to fail on `year`/`month`/`day`/`hour`/`bucket`/`truncate` partitioning with `NotInstalledError` after the table had already been created.
- **Fix**: `new_table_created` in the load result is `True` only when the load created the table. An existing table without snapshots (for example, left by a failed load) used to report `True`.
- **Docs**: the watermark example handles a missing table (`get_high_watermark` raises `NoSuchTableError`), REST catalog Option B no longer requires `ICEBERG_CREDENTIAL`, and the examples stack adds a Trino `iceberg_rest` catalog for Polaris.

# Release 0.3.0 — 2026-10-27

## Highlights
- Arrow `large_string` and `large_binary` types now map to Iceberg `string` and `binary`, so Polars `to_arrow()` output with top-level string/binary columns loads without a manual cast. Nested and dictionary types (`large_list`, Categorical) are still unsupported.
- **Observability**: Load results now include `conform_ms`, `write_ms`, `total_ms` stage timings and `casts_applied`, `casts_lossy`, `casts_failed` cast counters. Cast counters count distinct columns per load. Each load also emits one `METRIC: load_completed` info log with the same values.
- **New API**: `register_parquet_files()` registers existing Parquet files as Iceberg data files without rewriting them. It validates every footer before committing (unknown, missing or optional-but-required columns, embedded field IDs, nanosecond timestamps, missing partition statistics, relative and aliased paths), follows PyIceberg's type-promotion rules, supports partitioned tables when each file stays within one partition, and returns `files_added`, `rows_added`, `snapshot_id`, and `table_location`. Passing `check_duplicate_files=False` logs a warning and can duplicate rows.
- **New helper**: `get_high_watermark()` returns the maximum visible value of a column so callers can filter an incremental source with `> watermark`. `auto`/`metadata`/`scan` modes trade manifest-only reads for exact scans and fail loud when metadata bounds cannot be trusted.

## Breaking Changes
- **Load result dict has new keys**: `load_data_to_iceberg`, `load_batches_to_iceberg`, `load_ipc_stream_to_iceberg` and the `IcebergLoader` load methods now return `conform_ms`, `write_ms`, `total_ms`, `casts_applied`, `casts_lossy` and `casts_failed` in addition to the previous keys. Code that compares the whole result with `==` or validates it against a fixed set of keys must be updated; reading individual keys is unaffected.

## Test Plan
- `uv run pytest -v`
- `uv run ruff check . && uv run ruff format --check . && uv run ty check`
- `bash tools/run_examples_smoke.sh`

# Release 0.2.0 — 2026-10-26

## Highlights
- **New warning**: Casting a timestamp column to a coarser unit (e.g. `pa.timestamp('ns')` → microseconds) logs a warning when values lose precision. Previously the truncation was silent; the stored data is unchanged.
- **New warning**: When `format-version` is set explicitly (in `IcebergLoader(table_properties=...)` or `LoaderConfig.table_properties`) and an existing table has a different version, the loader logs a warning. The table keeps its version. A non-integer `format-version` for an existing table is ignored with a warning.
- **Fail fast on format v3**: If the new or existing table's format version is newer than the installed PyIceberg can write (currently 2), the loader raises `NotImplementedError` with a clear message before creating the table or writing data.
- **Types**: Iceberg format v3 types `timestamp_ns`, `timestamptz_ns` and `unknown` now map to Arrow `timestamp('ns')`, `timestamp('ns', tz='UTC')` and `null`. PyIceberg still cannot write v3 tables.
- **Dependencies**: `pydantic>=2.0,<3.0` is now a declared dependency (it was already required through PyIceberg).
- `BatchConformer` and `SchemaManager.ensure_table_exists` accept an optional `requested_format_version` argument.
- **Change**: `create_arrow_table_from_data` and `create_record_batches_from_dicts` now sort columns by name. Previously the order came from a `set` and could change between runs. New tables created from these helpers get sorted column order; existing tables are unaffected.
- **Docs**: Documentation audit. Fixed broken `replace_filter` examples, documented that the default `write_mode` is `'overwrite'`, that `commit_interval=0` commits every batch, and that `table_properties` apply only at table creation; added a "Behavior & gotchas" section and a full API reference. Architecture notes moved to `docs/ARCHITECTURE.md`; added `AGENTS.md` for coding agents.

## Breaking Changes
- **Minimum PyIceberg is now 0.10.0** (was 0.7.1). Upsert needs `Table.upsert` (PyIceberg 0.9) and `expire_snapshots` needs `table.maintenance` (PyIceberg 0.10); older versions failed with `AttributeError`.
- Loading into an existing format v3 table now raises `NotImplementedError` from iceberg-loader instead of `ValueError: Cannot write manifest for table version: 3` from PyIceberg. The load failed before as well; only the exception type and message changed.

## Test Plan
- `uv run pytest -v`
- `uv run ruff check . && uv run ruff format --check . && uv run ty check`
- `bash tools/run_examples_smoke.sh`

# Release 0.1.4 — 2026-10-02

## Highlights
- **Fix**: A mixed-schema batch buffer with a `day(_load_dttm)` partition now creates a partitioned table. The load timestamp column is added before table creation, so the partition spec can reference it.
- **Change**: For new tables, the load timestamp column now appears before columns added later by schema evolution. A first buffer of `[id]` then `[id, x]` with `load_timestamp` set now yields column order `['id', '_load_dttm', 'x']` (previously `['id', 'x', '_load_dttm']`).

## Breaking Changes
- New tables created with `load_timestamp` and `schema_evolution` have a different column order **only when** the first commit buffer contains batches with different schemas (i.e. `commit_interval > 1`). Existing tables are unaffected.

## Test Plan
- `uv run pytest -v`
- `uv run ruff check . && uv run ruff format --check . && uv run ty check`

## Release Steps
1) Ensure version is set to `0.1.4`.
2) Tag and push:
   ```bash
   git tag -a v0.1.4 -m "Release 0.1.4"
   git push origin v0.1.4
   ```

# Release 0.1.3 — 2026-04-23

## Highlights
- Added a Docker-backed examples smoke workflow in GitHub Actions.
- Added `tools/run_examples_smoke.sh` to bring up the local Trino/Hive/MinIO stack and run the fast local example subset.
- Fixed `examples/load_upsert.py` to reload the table before verifying the post-upsert snapshot.
- Made `examples/load_complex_json.py` and `examples/advanced_scenarios.py` deterministic for repeated runs and CI smoke checks.
- Updated example and contributor documentation to describe the smoke flow and its scope.

## Breaking Changes
- None.

## Test Plan
- `bash tools/run_examples_smoke.sh`

## Release Steps
1) Ensure version is set to `0.1.3`.
2) Tag and push:
   ```bash
   git tag -a v0.1.3 -m "Release 0.1.3"
   git push origin v0.1.3
   ```

# Release 0.0.7 — 2025-12-13

## Highlights
- **Refactored API**: Unified configuration via `LoaderConfig` for all loading functions.
- **Audit Field**: Added optional `load_timestamp` to track data loading time.
- **Fix**: Fixed state mutation issue where table properties persisted across calls.

## Breaking Changes
- The signatures of `load_data_to_iceberg`, `load_batches_to_iceberg`, and `load_ipc_stream_to_iceberg` have changed.
- Arguments like `write_mode`, `partition_col`, `schema_evolution` etc. are removed from function arguments.
- Pass these parameters via the `config` argument using `LoaderConfig`.

  ```python
  # Old
  load_data_to_iceberg(table, id, catalog, write_mode='append')

  # New
  config = LoaderConfig(write_mode='append')
  load_data_to_iceberg(table, id, catalog, config=config)
  ```

## New Features
- **Load Timestamp**: Automatically add a timestamp column (default `_load_dttm`) to loaded data.
  ```python
  config = LoaderConfig(load_timestamp=datetime.now())
  ```

## Release Steps
1) Ensure version is set to `0.0.7`.
2) Tag and push:
   ```bash
   git tag -a v0.0.7 -m "Release 0.0.7"
   git push origin v0.0.7
   ```

# Release 0.0.6 — 2025-12-12

## Highlights
- Restructured package into `core/`, `utils/`, `services/`.
- Removed legacy top-level modules; imports now use the new layout.
- Linting/typing/test pipelines updated to match the structure.

## Breaking Change
- Old internal paths like `iceberg_loader.arrow_utils`, `schema`, `maintenance`, `type_mappings`, `settings`, `logger`, `strategies` are removed.
- Use new imports:
  - `iceberg_loader.utils.arrow`
  - `iceberg_loader.core.config`, `iceberg_loader.core.schema`, `iceberg_loader.core.loader`
  - `iceberg_loader.services.logging`, `iceberg_loader.services.maintenance`
  - `iceberg_loader.utils.types`

## Release Steps
1) Ensure version is set to `0.0.6` in `src/iceberg_loader/__about__.py`.
2) Tag and push:
   ```bash
   git tag -a v0.0.6 -m "Release 0.0.6"
   git push origin v0.0.6
   ```
3) GitHub Actions (`.github/workflows/release.yml`) will run lint/types/tests, build, and publish (TestPyPI/PyPI if tokens present).

# Earlier releases

Releases 0.0.2–0.0.5 and 0.1.0–0.1.2 have no entries in this file. See the [git tags](https://github.com/vndv/iceberg-loader/tags) and [GitHub Releases](https://github.com/vndv/iceberg-loader/releases).
