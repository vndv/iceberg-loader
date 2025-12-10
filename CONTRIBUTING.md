# Contributing to iceberg-loader

Thank you for your interest in contributing to the project! This guide will help you get started quickly.

## Quick Start

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/YOUR_USERNAME/iceberg-loader.git
   cd iceberg-loader
   ```

2. **Install dependencies**
   
   We use [Hatch](https://hatch.pypa.io/latest/) for environment management:
   ```bash
   pip install hatch
   ```
   
   **Note**: This project requires Python 3.10-3.14. If you encounter installation issues:
   - Verify your Python version: `python --version`
   - Use Python 3.12 for best compatibility
   - `uv` may require explicit Python version: `uv venv --python 3.12`

3. **Set up the development environment**
   ```bash
   hatch shell
   ```

4. **Run linters and tests**
   ```bash
   hatch run lint
   hatch run test
   ```

## Development Workflow

### Running Tests

```bash
# Run all tests
hatch run test

# Run with coverage report
hatch run test -- --cov=iceberg_loader --cov-report=html

# Run specific test file
hatch run test tests/test_iceberg_loader.py

# Run tests with verbose output
hatch run test -v
```

### Code Quality

```bash
# Run linter (ruff)
hatch run lint

# Auto-fix linting issues
hatch run ruff check --fix .

# Format code
hatch run ruff format .

# Type checking
hatch run types:check
```

### Running Examples

Examples require a local Iceberg environment with Docker:

```bash
cd examples
docker-compose up -d
cd ..

# Run individual examples
hatch run python examples/load_example.py
hatch run python examples/advanced_scenarios.py
hatch run python examples/load_with_commits.py
```

## Code Style Guidelines

- **Python Version**: 3.10+ (3.12 recommended)
- **Linter**: `ruff` for linting and formatting
- **Type Hints**: Add type hints to all public functions and classes
- **Docstrings**: Use Google-style docstrings for public APIs
- **Logging**: Use `logging.getLogger(__name__)` pattern
- **No Comments**: Code should be self-explanatory (unless complex logic requires explanation)

### Example

```python
import logging
from typing import Any

logger = logging.getLogger(__name__)


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
- **Mocking**: Mock external dependencies (catalog, tables) appropriately

### Example Test

```python
def test_load_data_with_schema_evolution():
    """Test that schema evolution adds new columns correctly."""
    # Arrange
    catalog = get_mock_catalog()
    data = create_test_data()
    
    # Act
    result = load_data_to_iceberg(data, ("db", "table"), catalog, schema_evolution=True)
    
    # Assert
    assert result['rows_loaded'] == 3
```

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

## Release Process

Releases are automated via GitHub Actions:

1. Ensure all tests pass on `main`
2. Update version in `src/iceberg_loader/__about__.py`
3. Create and push a git tag:
   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```
4. GitHub Actions will automatically:
   - Build the package
   - Run tests
   - Publish to PyPI

## Getting Help

- **Questions**: Open a [Discussion](https://github.com/IvanMatveev/iceberg-loader/discussions)
- **Bugs**: Open an [Issue](https://github.com/IvanMatveev/iceberg-loader/issues)
- **Features**: Open an [Issue](https://github.com/IvanMatveev/iceberg-loader/issues) with the `enhancement` label

## Code of Conduct

- Be respectful and inclusive
- Welcome newcomers
- Accept constructive criticism
- Focus on what is best for the community

## Recognition

All contributors will be recognized in the project README. Thank you for making iceberg-loader better! 🎉
