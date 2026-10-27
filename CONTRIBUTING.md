# Contributing to iceberg-loader

Thank you for your interest in contributing to the project! This guide will help you get started quickly.

## Quick Start

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/iceberg-loader.git
   cd iceberg-loader
   ```

2. **Install uv**

   We use [uv](https://docs.astral.sh/uv/) for environment management:
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

   **Note**: This project requires Python 3.10-3.14. If you need a specific version:
   ```bash
   uv venv --python 3.12
   ```

3. **Set up the development environment**
   ```bash
   uv sync
   ```

4. **Run linters and tests**
   ```bash
   uv run ruff check . && uv run ruff format --check . && uv run ty check
   uv run pytest
   ```

## Development Workflow

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage report
uv run pytest --cov=iceberg_loader --cov-report=html

# Run specific test file
uv run pytest tests/test_iceberg_loader.py

# Run tests with verbose output
uv run pytest -v
```

### Code Quality

```bash
# Run all checks (lint + format check + types)
uv run ruff check . && uv run ruff format --check . && uv run ty check

# Auto-fix linting issues
uv run ruff check --fix .

# Format code
uv run ruff format .

# Type checking
uv run ty check
```

### Running Examples

Examples require a local Iceberg environment with Docker:

```bash
cd examples
docker compose up -d
cd ..

# Run individual examples
uv run python examples/load_upsert.py
uv run python examples/advanced_scenarios.py
uv run python examples/load_with_commits.py

# Run the Docker-backed smoke subset
bash tools/run_examples_smoke.sh
```

The smoke subset intentionally excludes `examples/load_from_api.py` because it depends on an external API,
and `examples/load_stream.py` because it is much heavier than a normal CI smoke run.

## Code Style Guidelines

- **Python Version**: 3.10+ (3.12 recommended)
- **Linter**: `ruff` for linting and formatting
- **Type Hints**: Add type hints to all public functions and classes
- **Docstrings**: Give public functions and classes a short docstring that says what they do; add `Args:`/`Returns:`/`Raises:` sections when the behavior is not obvious from the signature (see `get_rest_catalog` in `catalog.py`)
- **Logging**: Use the package logger: `from iceberg_loader.services.logging import logger`. It writes to the `iceberg_loader` logger, which does not propagate to the root logger
- **No Comments**: Code should be self-explanatory (unless complex logic requires explanation)

### Example

```python
from typing import Any

from iceberg_loader.services.logging import logger


def load_data(table_id: tuple[str, str], data: Any) -> dict[str, Any]:
    """
    Load data into an Iceberg table.

    Args:
        table_id: Table identifier as (namespace, table_name)
        data: Data to load

    Returns:
        Dictionary with loading results
    """
    logger.info('Loading data into table %s', table_id)
    # Implementation
    return {'rows_loaded': 100}
```

## Testing Guidelines

- **New Feature → New Test**: Every new feature must include corresponding tests
- **Test Coverage**: Aim for 90%+ coverage for new code
- **Test Structure**: Use descriptive test names that explain what is being tested
- **Fixtures**: Use pytest fixtures for common test setups
- **Catalog**: Test against a real catalog, not mocks. The `sql_catalog` fixture (`tests/conftest.py`) gives each test an isolated sqlite-backed `SqlCatalog` with a `default` namespace
- **Log assertions**: The `iceberg_loader` logger does not propagate to root, so `caplog` alone misses its records; use the `loader_warnings` fixture

### Example Test

```python
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog

from iceberg_loader import LoaderConfig, load_data_to_iceberg


def test_schema_evolution_adds_new_column(sql_catalog: SqlCatalog) -> None:
    config = LoaderConfig(write_mode='append', schema_evolution=True)
    load_data_to_iceberg(pa.table({'id': [1]}), ('default', 'events'), sql_catalog, config=config)

    result = load_data_to_iceberg(pa.table({'id': [2], 'x': ['a']}), ('default', 'events'), sql_catalog, config=config)

    assert result['rows_loaded'] == 1
    assert sql_catalog.load_table(('default', 'events')).schema().find_field('x')
```

Set `write_mode` explicitly in tests: the default is `'overwrite'`.

## Pull Request Process

1. **Branch Naming**: Use descriptive branch names
   - `feature/add-streaming-support`
   - `fix/schema-evolution-bug`
   - `docs/update-api-reference`

2. **Commit Messages**: Write clear commit messages
   - Use present tense: "Add feature" not "Added feature"
   - Keep first line under 72 characters
   - Add detailed description if needed

3. **Pull Request Description**
   - Describe the changes and motivation
   - Link to related issues
   - Include test results
   - Add screenshots/logs if relevant

4. **Keep PRs Focused**:
   - One feature/fix per PR
   - Break large changes into smaller PRs
   - Rebase on main before submitting

5. **Code Review**
   - Address all review comments
   - Be open to feedback
   - Update PR based on suggestions

## Release Checklist

This is the single release checklist; other documents link here.

1. Bump the version in `pyproject.toml` and `src/iceberg_loader/__about__.py` (they must match).
2. Move the `Unreleased` section of `RELEASE.md` under a `# Release X.Y.Z — YYYY-MM-DD` heading, with highlights and breaking changes.
3. Run `uv lock`, commit `uv.lock` if it changes, then verify it with `uv lock --locked`.
4. Run lint, types, and tests:
   ```bash
   uv run ruff check . && uv run ruff format --check . && uv run ty check
   uv run pytest
   ```
5. Merge to `main`, then create an annotated tag and push it:
   ```bash
   git tag -a vX.Y.Z -m "Release X.Y.Z"
   git push origin vX.Y.Z
   ```
6. The tag triggers `.github/workflows/release.yml`: it runs lint, types, and tests, builds the package, and publishes to PyPI. Do not upload with `twine` by hand.

## Getting Help

- **Questions**: Open an [Issue](https://github.com/vndv/iceberg-loader/issues)
- **Bugs**: Open an [Issue](https://github.com/vndv/iceberg-loader/issues)
- **Features**: Open an [Issue](https://github.com/vndv/iceberg-loader/issues) with the `enhancement` label

## Code of Conduct

- Be respectful and inclusive
- Welcome newcomers
- Accept constructive criticism
- Focus on what is best for the community

## Recognition

All contributors will be recognized in the project README. Thank you for making iceberg-loader better! 🎉
