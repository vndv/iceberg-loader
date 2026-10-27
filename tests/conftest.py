import logging
from collections.abc import Callable, Iterator
from pathlib import Path

import pytest
from pyiceberg.catalog.sql import SqlCatalog

from iceberg_loader.services.logging import get_logger


@pytest.fixture()
def sql_catalog(tmp_path: Path) -> SqlCatalog:
    """Real Iceberg catalog backed by sqlite and a local warehouse; isolated per test."""
    catalog = SqlCatalog(
        'test',
        uri=f'sqlite:///{tmp_path}/catalog.db',
        warehouse=f'file://{tmp_path}/warehouse',
    )
    catalog.create_namespace('default')
    return catalog


@pytest.fixture()
def loader_warnings(caplog: pytest.LogCaptureFixture) -> Iterator[Callable[[], list[str]]]:
    """Returns WARNING+ messages from the iceberg_loader logger.

    configure_logging() (called by some test modules) turns propagation off, so it is forced on for the test:
    caplog's handler sits on the root logger.
    """
    loader_logger = get_logger()
    propagate = loader_logger.propagate
    loader_logger.propagate = True
    try:
        yield lambda: [
            r.getMessage()
            for r in caplog.records
            if r.levelno >= logging.WARNING and r.name.startswith('iceberg_loader')
        ]
    finally:
        loader_logger.propagate = propagate
