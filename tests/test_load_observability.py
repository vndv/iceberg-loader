import io
import logging
import sys
from collections.abc import Callable, Iterator
from typing import Any

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from iceberg_loader import LoaderConfig, load_batches_to_iceberg, load_data_to_iceberg

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
    assert result['casts_applied'] == 1
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
    caplog.set_level(logging.DEBUG, logger='iceberg_loader')

    result = load_data_to_iceberg(pa.table({'id': [1, 2]}), TID, sql_catalog, APPEND)

    records = [record for record in caplog.records if record.getMessage() == 'METRIC: load_completed']
    assert len(records) == 1
    record = records[0]
    assert record.levelno == logging.DEBUG
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


@pytest.fixture()
def restore_loader_logger() -> Iterator[logging.Logger]:
    """Restores the iceberg_loader logger and the module-level LOGGER that a test reconfigures."""
    import iceberg_loader.services.logging as logging_mod

    loader_logger = logging.getLogger('iceberg_loader')
    saved = (logging_mod.LOGGER, loader_logger.handlers[:], loader_logger.propagate, loader_logger.level)
    try:
        yield loader_logger
    finally:
        logging_mod.LOGGER, loader_logger.handlers, loader_logger.propagate, level = saved
        loader_logger.setLevel(level)


def test_unconfigured_logger_propagates_to_root_without_own_output(
    restore_loader_logger: logging.Logger,
    caplog: pytest.LogCaptureFixture,
    capsys: pytest.CaptureFixture[str],
) -> None:
    import iceberg_loader.services.logging as logging_mod

    # Default state on import, as if configure_logging() had never run (another test module calls it).
    logging_mod.LOGGER = None
    restore_loader_logger.handlers = [logging.NullHandler()]
    restore_loader_logger.propagate = True
    restore_loader_logger.setLevel(logging.NOTSET)
    caplog.set_level(logging.DEBUG)

    logging_mod.metrics('load_completed', {'rows_loaded': 0})
    logging_mod.info('hello')

    assert [r.getMessage() for r in caplog.records] == ['METRIC: load_completed', 'hello']
    assert logging_mod.LOGGER is None
    assert capsys.readouterr().err == ''


def float_batches(count: int) -> list[pa.RecordBatch]:
    return [pa.record_batch({'id': [i], 'v': pa.array([i + 0.5], pa.float64())}) for i in range(count)]


@pytest.mark.parametrize(
    'config',
    [
        LoaderConfig(write_mode='append'),
        LoaderConfig(write_mode='append', commit_interval=10),
        LoaderConfig(write_mode='append', commit_interval=4),
        LoaderConfig(write_mode='append', commit_interval=10, schema_evolution=True),
    ],
    ids=['per-batch', 'one-buffer', 'three-buffers', 'schema-evolution'],
)
def test_cast_counters_count_columns_once_per_load(sql_catalog: SqlCatalog, config: LoaderConfig) -> None:
    load_data_to_iceberg(pa.table({'id': [0], 'v': [0]}), TID, sql_catalog, APPEND)

    result = load_batches_to_iceberg(iter(float_batches(10)), TID, sql_catalog, config)

    assert result['batches_processed'] == 10
    assert (result['casts_applied'], result['casts_lossy'], result['casts_failed']) == (1, 1, 0)
    assert len(read_rows(sql_catalog)) == 11


def test_cast_counters_ignore_input_chunking(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(pa.table({'id': [0], 'v': [0]}), TID, sql_catalog, APPEND)
    chunked = pa.Table.from_batches(float_batches(10))
    assert chunked.column('v').num_chunks == 10

    result = load_data_to_iceberg(chunked, TID, sql_catalog, APPEND)

    assert (result['casts_applied'], result['casts_lossy'], result['casts_failed']) == (1, 1, 0)


@pytest.mark.parametrize(
    'config',
    [
        LoaderConfig(write_mode='append'),
        LoaderConfig(write_mode='overwrite'),
        LoaderConfig(write_mode='upsert', join_cols=['id']),
        LoaderConfig(write_mode='append', replace_filter='id >= 0'),
    ],
    ids=['append', 'overwrite', 'upsert', 'replace-filter'],
)
def test_counters_and_timings_in_every_write_mode(sql_catalog: SqlCatalog, config: LoaderConfig) -> None:
    load_data_to_iceberg(pa.table({'id': [0], 'v': [0]}), TID, sql_catalog, APPEND)

    result = load_data_to_iceberg(pa.Table.from_batches(float_batches(3)), TID, sql_catalog, config)

    assert (result['casts_applied'], result['casts_lossy'], result['casts_failed']) == (1, 1, 0)
    assert 0 <= result['conform_ms'] + result['write_ms'] <= result['total_ms']
    assert sorted(row['v'] for row in read_rows(sql_catalog) if row['id'] in (1, 2)) == [1, 2]


class _TTY(io.StringIO):
    def isatty(self) -> bool:
        return True


def _record(level: int) -> logging.LogRecord:
    return logging.LogRecord('iceberg_loader', level, __file__, 1, 'hello', None, None)


def test_text_formatter_colors_level_name_without_touching_the_record() -> None:
    import iceberg_loader.services.logging as logging_mod

    record = _record(logging.WARNING)
    colored = logging_mod.TextFormatter(fmt='[{levelname}] {message}', style='{', use_color=True).format(record)
    plain = logging_mod.TextFormatter(fmt='[{levelname}] {message}', style='{').format(record)

    assert colored == '[\x1b[33mWARNING\x1b[0m] hello'
    assert plain == '[WARNING] hello'
    assert record.levelname == 'WARNING'


@pytest.mark.parametrize(
    ('stream', 'no_color', 'expect_color'),
    [(_TTY(), None, True), (_TTY(), '1', False), (io.StringIO(), None, False)],
)
def test_configure_logging_colors_only_a_terminal_without_no_color(
    restore_loader_logger: logging.Logger,
    monkeypatch: pytest.MonkeyPatch,
    stream: io.StringIO,
    no_color: str | None,
    expect_color: bool,
) -> None:
    import iceberg_loader.services.logging as logging_mod

    monkeypatch.setattr(sys, 'stderr', stream)
    if no_color is None:
        monkeypatch.delenv('NO_COLOR', raising=False)
    else:
        monkeypatch.setenv('NO_COLOR', no_color)

    logging_mod.configure_logging()
    logging_mod.info('hello')

    assert ('\x1b[32mINFO\x1b[0m' in stream.getvalue()) is expect_color
