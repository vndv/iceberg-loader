import logging
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from iceberg_loader import LoaderConfig, load_data_to_iceberg

TID = ('default', 'events')
APPEND = LoaderConfig(write_mode='append')


def read_rows(catalog: SqlCatalog, identifier: tuple[str, str] = TID) -> list[dict[str, Any]]:
    return catalog.load_table(identifier).scan().to_arrow().to_pylist()


def test_clean_load_reports_zero_casts_and_timings(sql_catalog: SqlCatalog) -> None:
    result = load_data_to_iceberg(pa.table({'id': [1, 2]}), TID, sql_catalog, APPEND)

    for key in ('conform_ms', 'write_ms', 'total_ms'):
        assert result[key] >= 0
    assert result['casts_applied'] == 0
    assert result['casts_lossy'] == 0
    assert result['casts_failed'] == 0
    assert read_rows(sql_catalog) == [{'id': 1}, {'id': 2}]


def test_failed_cast_is_counted_and_stored_as_null(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(pa.table({'v': [1]}), TID, sql_catalog, APPEND)

    result = load_data_to_iceberg(pa.table({'v': ['not-an-int']}), TID, sql_catalog, APPEND)

    assert result['casts_failed'] == 1
    assert result['casts_applied'] == 0
    assert result['casts_lossy'] == 0
    assert sorted(read_rows(sql_catalog), key=lambda row: row['v'] is None) == [{'v': 1}, {'v': None}]


def test_lossy_timestamp_cast_is_counted_and_stored_truncated(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(
        pa.table({'ts': pa.array([1_700_000_000_000_000], type=pa.timestamp('us'))}),
        TID,
        sql_catalog,
        APPEND,
    )

    result = load_data_to_iceberg(
        pa.table({'ts': pa.array([1_700_000_000_123_456_789], type=pa.timestamp('ns'))}),
        TID,
        sql_catalog,
        APPEND,
    )

    assert result['casts_lossy'] == 1
    assert result['casts_applied'] >= 1
    assert result['casts_failed'] == 0
    stored = sorted(sql_catalog.load_table(TID).scan().to_arrow().column('ts').cast(pa.int64()).to_pylist())
    assert stored == [1_700_000_000_000_000, 1_700_000_000_123_456]


def test_lossy_float_to_int_cast_is_counted_and_stored_truncated(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(pa.table({'v': [1]}), TID, sql_catalog, APPEND)

    result = load_data_to_iceberg(pa.table({'v': pa.array([1.9], type=pa.float64())}), TID, sql_catalog, APPEND)

    assert result['casts_applied'] == 1
    assert result['casts_lossy'] == 1
    assert result['casts_failed'] == 0
    assert read_rows(sql_catalog) == [{'v': 1}, {'v': 1}]


def test_load_completed_metric_is_logged(
    sql_catalog: SqlCatalog,
    loader_warnings: Callable[[], list[str]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger='iceberg_loader')

    result = load_data_to_iceberg(pa.table({'id': [1, 2]}), TID, sql_catalog, APPEND)

    records = [record for record in caplog.records if record.getMessage() == 'METRIC: load_completed']
    assert len(records) == 1
    record = records[0]
    assert record.levelno == logging.INFO
    payload: dict[str, Any] = record.__dict__['metrics']
    assert set(payload) == {
        'rows_loaded',
        'batches_processed',
        'write_mode',
        'conform_ms',
        'write_ms',
        'total_ms',
        'casts_applied',
        'casts_lossy',
        'casts_failed',
    }
    assert payload['rows_loaded'] == 2
    assert payload['batches_processed'] == 1
    assert payload['write_mode'] == 'append'
    assert payload['conform_ms'] == result['conform_ms']
    assert payload['write_ms'] == result['write_ms']
    assert payload['total_ms'] == result['total_ms']
    assert payload['casts_applied'] == result['casts_applied']
    assert payload['casts_lossy'] == result['casts_lossy']
    assert payload['casts_failed'] == result['casts_failed']
    assert loader_warnings() == []
