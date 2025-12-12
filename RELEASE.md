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
