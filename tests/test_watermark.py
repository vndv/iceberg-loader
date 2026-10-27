import datetime as dt
import decimal
import uuid
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.manifest import DataFile, DataFileContent

from iceberg_loader import LoaderConfig, get_high_watermark, load_data_to_iceberg

TABLE = ('default', 'events')


def load(
    catalog: SqlCatalog,
    data: pa.Table,
    *,
    table_id: tuple[str, str] = TABLE,
    partition_col: str | None = None,
) -> None:
    load_data_to_iceberg(
        data,
        table_id,
        catalog,
        LoaderConfig(write_mode='append', partition_col=partition_col),
    )


def add_position_delete(catalog: SqlCatalog, table_id: tuple[str, str], position: int) -> None:
    """Attach a real position-delete file to the first data file of the table.

    PyIceberg 0.12 falls back to copy-on-write for ``Table.delete``, so it never produces delete
    files. This helper writes a genuine Iceberg position-delete file and commits it through
    PyIceberg's own transaction/manifest APIs; it is the same representation a merge-on-read
    writer would produce.
    """
    table = catalog.load_table(table_id)
    data_file = table.inspect.data_files().to_pylist()[0]
    delete_payload = pa.table(
        {
            'file_path': pa.array([data_file['file_path']], pa.string()),
            'pos': pa.array([position], pa.int64()),
        },
    )
    delete_location = f'{table.location()}/data/position-delete-{uuid.uuid4().hex}.parquet'
    local_path = Path(urlparse(delete_location).path)
    pq.write_table(delete_payload, local_path)

    delete_file = DataFile.from_args(
        content=DataFileContent.POSITION_DELETES,
        file_path=delete_location,
        file_format='PARQUET',
        partition={},
        record_count=delete_payload.num_rows,
        file_size_in_bytes=local_path.stat().st_size,
        column_sizes={},
        value_counts={},
        null_value_counts={},
        nan_value_counts={},
        lower_bounds={},
        upper_bounds={},
        key_metadata=None,
        split_offsets=None,
        equality_ids=None,
        sort_order_id=None,
        spec_id=0,
    )
    with table.transaction() as transaction, transaction.update_snapshot().fast_append() as append:
        append.append_data_file(delete_file)


def test_no_visible_rows_after_delete_returns_none(sql_catalog: SqlCatalog) -> None:
    load(sql_catalog, pa.table({'id': pa.array([1], pa.int64())}))
    sql_catalog.load_table(TABLE).delete('id = 1')

    assert get_high_watermark(TABLE, 'id', sql_catalog) is None


def test_zero_data_files_returns_none_in_metadata_mode(sql_catalog: SqlCatalog) -> None:
    sql_catalog.create_table(TABLE, schema=pa.schema([pa.field('name', pa.string())]))

    assert get_high_watermark(TABLE, 'name', sql_catalog, mode='metadata') is None


def test_int_column_metadata_and_auto(sql_catalog: SqlCatalog) -> None:
    load(sql_catalog, pa.table({'id': pa.array([5, 1, 9], pa.int64())}))

    assert get_high_watermark(TABLE, 'id', sql_catalog, mode='metadata') == 9
    assert get_high_watermark(TABLE, 'id', sql_catalog, mode='auto') == 9


def test_date_and_timestamp_max_values_and_types(sql_catalog: SqlCatalog) -> None:
    timestamps = [
        dt.datetime(2024, 1, 1, 10, 0),
        dt.datetime(2024, 3, 2, 9, 0),
        dt.datetime(2024, 2, 1, 12, 0),
    ]
    aware_timestamps = [value.replace(tzinfo=dt.timezone.utc) for value in timestamps]
    load(
        sql_catalog,
        pa.table(
            {
                'id': pa.array([1, 2, 3], pa.int64()),
                'event_date': pa.array([dt.date(2024, 1, 1), dt.date(2024, 3, 2), dt.date(2024, 2, 1)], pa.date32()),
                'event_ts': pa.array(timestamps, pa.timestamp('us')),
                'event_ts_utc': pa.array(aware_timestamps, pa.timestamp('us', tz='UTC')),
            },
        ),
    )

    event_date = get_high_watermark(TABLE, 'event_date', sql_catalog, mode='metadata')
    assert event_date == dt.date(2024, 3, 2)
    assert isinstance(event_date, dt.date)

    event_ts = get_high_watermark(TABLE, 'event_ts', sql_catalog, mode='metadata')
    assert event_ts == dt.datetime(2024, 3, 2, 9, 0)
    assert isinstance(event_ts, dt.datetime)
    assert event_ts.tzinfo is None

    event_ts_utc = get_high_watermark(TABLE, 'event_ts_utc', sql_catalog, mode='auto')
    assert event_ts_utc == dt.datetime(2024, 3, 2, 9, 0, tzinfo=dt.timezone.utc)
    assert event_ts_utc.tzinfo is not None


def test_string_scan_and_auto_return_exact_max(sql_catalog: SqlCatalog) -> None:
    load(sql_catalog, pa.table({'name': pa.array(['alpha', 'zeta', 'mu'], pa.string())}))

    assert get_high_watermark(TABLE, 'name', sql_catalog, mode='scan') == 'zeta'
    assert get_high_watermark(TABLE, 'name', sql_catalog, mode='auto') == 'zeta'


def test_metadata_refuses_string_column(sql_catalog: SqlCatalog) -> None:
    load(sql_catalog, pa.table({'name': pa.array(['alpha', 'zeta'], pa.string())}))

    with pytest.raises(ValueError, match='scan'):
        get_high_watermark(TABLE, 'name', sql_catalog, mode='metadata')


def test_unknown_column_names_the_column_and_top_level_columns(sql_catalog: SqlCatalog) -> None:
    load(sql_catalog, pa.table({'id': pa.array([1], pa.int64()), 'name': pa.array(['a'], pa.string())}))

    with pytest.raises(ValueError) as exc_info:
        get_high_watermark(TABLE, 'missing', sql_catalog)
    message = str(exc_info.value)
    assert 'missing' in message
    assert 'id' in message
    assert 'name' in message


def test_dotted_nested_column_is_rejected(sql_catalog: SqlCatalog) -> None:
    load(sql_catalog, pa.table({'id': pa.array([1], pa.int64())}))

    with pytest.raises(ValueError) as exc_info:
        get_high_watermark(TABLE, 'id.value', sql_catalog)
    assert 'id.value' in str(exc_info.value)


def test_all_null_column_returns_none_in_both_modes(sql_catalog: SqlCatalog) -> None:
    load(
        sql_catalog,
        pa.table(
            {
                'id': pa.array([1, 2], pa.int64()),
                'nothing': pa.array([None, None], pa.int64()),
            },
        ),
    )

    assert get_high_watermark(TABLE, 'nothing', sql_catalog, mode='metadata') is None
    assert get_high_watermark(TABLE, 'nothing', sql_catalog, mode='scan') is None


def test_delete_files_force_auto_to_scan_visible_rows(sql_catalog: SqlCatalog) -> None:
    load(sql_catalog, pa.table({'id': pa.array([1, 2, 3], pa.int64()), 'v': pa.array([10, 20, 30], pa.int64())}))
    data_file = sql_catalog.load_table(TABLE).inspect.data_files().to_pylist()[0]
    assert data_file['readable_metrics']['v']['upper_bound'] == 30

    add_position_delete(sql_catalog, TABLE, position=2)

    assert sql_catalog.load_table(TABLE).inspect.delete_files().num_rows > 0
    with pytest.raises(ValueError, match='delete file'):
        get_high_watermark(TABLE, 'v', sql_catalog, mode='metadata')
    assert get_high_watermark(TABLE, 'v', sql_catalog, mode='auto') == 20


def test_invalid_mode_lists_allowed_modes(sql_catalog: SqlCatalog) -> None:
    load(sql_catalog, pa.table({'id': pa.array([1], pa.int64())}))

    with pytest.raises(ValueError, match='auto, metadata, scan'):
        get_high_watermark(TABLE, 'id', sql_catalog, mode=cast(Any, 'bogus'))


def test_partitioned_table_auto_returns_max(sql_catalog: SqlCatalog) -> None:
    timestamps = [
        dt.datetime(2024, 1, 1, 1, 0),
        dt.datetime(2024, 1, 2, 3, 0),
        dt.datetime(2024, 1, 3, 2, 0),
    ]
    load(
        sql_catalog,
        pa.table({'id': pa.array([1, 2, 3], pa.int64()), 'ts': pa.array(timestamps, pa.timestamp('us'))}),
        partition_col='day(ts)',
    )

    assert get_high_watermark(TABLE, 'ts', sql_catalog, mode='auto') == dt.datetime(2024, 1, 3, 2, 0)


def test_missing_table_raises(sql_catalog: SqlCatalog) -> None:
    with pytest.raises(NoSuchTableError):
        get_high_watermark(('default', 'missing'), 'id', sql_catalog)


def test_decimal_column_round_trips_in_both_modes(sql_catalog: SqlCatalog) -> None:
    load(
        sql_catalog,
        pa.table(
            {
                'amount': pa.array(
                    [decimal.Decimal('1.25'), decimal.Decimal('9.99'), decimal.Decimal('3.50')],
                    pa.decimal128(9, 2),
                ),
            },
        ),
    )

    metadata_watermark = get_high_watermark(TABLE, 'amount', sql_catalog, mode='metadata')
    scan_watermark = get_high_watermark(TABLE, 'amount', sql_catalog, mode='scan')

    for watermark in (metadata_watermark, scan_watermark):
        assert watermark == decimal.Decimal('9.99')
        assert isinstance(watermark, decimal.Decimal)


def test_nested_column_raises_value_error_in_every_mode(sql_catalog: SqlCatalog) -> None:
    sql_catalog.create_table(
        TABLE,
        schema=pa.schema(
            [
                pa.field('id', pa.int64()),
                pa.field('payload', pa.struct([pa.field('a', pa.int64())])),
            ],
        ),
    )

    with pytest.raises(ValueError, match='not a primitive'):
        get_high_watermark(TABLE, 'payload', sql_catalog, mode='scan')
    with pytest.raises(ValueError, match='not a primitive'):
        get_high_watermark(TABLE, 'payload', sql_catalog, mode='metadata')


def test_new_column_without_upper_bound_in_older_file(sql_catalog: SqlCatalog) -> None:
    load(sql_catalog, pa.table({'id': pa.array([1], pa.int64()), 'v': pa.array([10], pa.int64())}))
    load_data_to_iceberg(
        pa.table(
            {
                'id': pa.array([2], pa.int64()),
                'v': pa.array([20], pa.int64()),
                'added': pa.array([7], pa.int64()),
            },
        ),
        TABLE,
        sql_catalog,
        LoaderConfig(write_mode='append', schema_evolution=True),
    )

    with pytest.raises(ValueError, match='no upper bound'):
        get_high_watermark(TABLE, 'added', sql_catalog, mode='metadata')
    assert get_high_watermark(TABLE, 'added', sql_catalog, mode='auto') == 7
