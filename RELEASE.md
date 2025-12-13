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
- See `TODO.md` for migration examples.

## Test Plan
- `tox -e lint`
- `tox -e py310-tests`
- `tox -e py311-tests`
- `tox -e py312-tests`
- `tox -e py313-tests`
- `tox -e py314-tests`

## Release Steps
1) Ensure version is set to `0.0.6` in `src/iceberg_loader/__about__.py`.
2) Tag and push:
   ```bash
   git tag -a v0.0.6 -m "Release 0.0.6"
   git push origin v0.0.6
   ```
3) GitHub Actions (`.github/workflows/release.yml`) will run lint/types/tests, build, and publish (TestPyPI/PyPI if tokens present).
