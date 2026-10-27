from datetime import datetime
from typing import Any

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from iceberg_loader import IcebergLoader, LoaderConfig, load_batches_to_iceberg, load_data_to_iceberg

TID = ('default', 'events')
APPEND = LoaderConfig(write_mode='append')


def read_rows(catalog: SqlCatalog, identifier: tuple[str, str] = TID) -> list[dict[str, Any]]:
    return catalog.load_table(identifier).scan().to_arrow().sort_by('id').to_pylist()


def batch(**columns: list[Any]) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(columns)


def test_append_creates_table_and_reports_result(sql_catalog: SqlCatalog) -> None:
    result = load_data_to_iceberg(pa.table({'id': [1, 2], 'v': ['a', 'b']}), TID, sql_catalog, APPEND)

    table = sql_catalog.load_table(TID)
    snapshot = table.current_snapshot()
    assert snapshot is not None
    assert read_rows(sql_catalog) == [{'id': 1, 'v': 'a'}, {'id': 2, 'v': 'b'}]
    timing_keys = {'conform_ms', 'write_ms', 'total_ms'}
    assert {key: value for key, value in result.items() if key not in timing_keys} == {
        'rows_loaded': 2,
        'write_mode': 'append',
        'partition_col': 'none',
        'table_location': table.location(),
        'snapshot_id': snapshot.snapshot_id,
        'batches_processed': 1,
        'new_table_created': True,
        'casts_applied': 0,
        'casts_lossy': 0,
        'casts_failed': 0,
    }
    assert all(result[key] >= 0 for key in timing_keys)


def test_append_to_existing_table_keeps_rows(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, APPEND)
    result = load_data_to_iceberg(pa.table({'id': [2]}), TID, sql_catalog, APPEND)

    assert read_rows(sql_catalog) == [{'id': 1}, {'id': 2}]
    assert result['new_table_created'] is False


def test_overwrite_replaces_existing_rows(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(pa.table({'id': [1, 2]}), TID, sql_catalog, APPEND)
    load_data_to_iceberg(pa.table({'id': [3]}), TID, sql_catalog, LoaderConfig(write_mode='overwrite'))

    assert read_rows(sql_catalog) == [{'id': 3}]


def test_overwrite_keeps_every_batch_of_the_stream(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, APPEND)
    config = LoaderConfig(write_mode='overwrite', commit_interval=1)

    result = load_batches_to_iceberg(iter([batch(id=[2]), batch(id=[3])]), TID, sql_catalog, config)

    assert read_rows(sql_catalog) == [{'id': 2}, {'id': 3}]
    assert result['batches_processed'] == 2


def test_replace_filter_deletes_matching_rows_then_appends(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(pa.table({'id': [1, 2], 'v': ['a', 'b']}), TID, sql_catalog, APPEND)
    config = LoaderConfig(write_mode='append', replace_filter='id = 2')

    load_data_to_iceberg(pa.table({'id': [2], 'v': ['B']}), TID, sql_catalog, config)

    assert read_rows(sql_catalog) == [{'id': 1, 'v': 'a'}, {'id': 2, 'v': 'B'}]


def test_upsert_updates_matches_and_inserts_new_rows(sql_catalog: SqlCatalog) -> None:
    config = LoaderConfig(write_mode='upsert', join_cols=['id'])
    first = load_data_to_iceberg(pa.table({'id': [1, 2], 'v': ['a', 'b']}), TID, sql_catalog, config)

    load_data_to_iceberg(pa.table({'id': [2, 3], 'v': ['B', 'c']}), TID, sql_catalog, config)

    assert first['new_table_created'] is True
    assert read_rows(sql_catalog) == [{'id': 1, 'v': 'a'}, {'id': 2, 'v': 'B'}, {'id': 3, 'v': 'c'}]


@pytest.mark.parametrize('commit_interval', [0, 2])
def test_schema_evolution_adds_new_column_midstream(sql_catalog: SqlCatalog, commit_interval: int) -> None:
    batches = [batch(id=[1], v=['a']), batch(id=[2], v=['b'], extra=['x'])]
    config = LoaderConfig(write_mode='append', schema_evolution=True, commit_interval=commit_interval)

    result = load_batches_to_iceberg(iter(batches), TID, sql_catalog, config)

    assert read_rows(sql_catalog) == [
        {'id': 1, 'v': 'a', 'extra': None},
        {'id': 2, 'v': 'b', 'extra': 'x'},
    ]
    assert result['new_table_created'] is True


def test_mixed_buffer_without_schema_evolution_raises(sql_catalog: SqlCatalog) -> None:
    batches = [batch(id=[1]), batch(id=[2], extra=['x'])]

    with pytest.raises(pa.ArrowInvalid):
        load_batches_to_iceberg(iter(batches), TID, sql_catalog, LoaderConfig(write_mode='append', commit_interval=2))

    assert not sql_catalog.table_exists(TID)


def test_without_schema_evolution_unknown_columns_are_dropped(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, APPEND)
    load_data_to_iceberg(pa.table({'id': [2], 'extra': ['x']}), TID, sql_catalog, APPEND)

    assert sql_catalog.load_table(TID).schema().column_names == ['id']
    assert read_rows(sql_catalog) == [{'id': 1}, {'id': 2}]


def test_missing_columns_are_filled_with_nulls(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(pa.table({'id': [1], 'v': ['a']}), TID, sql_catalog, APPEND)
    load_data_to_iceberg(pa.table({'id': [2]}), TID, sql_catalog, APPEND)

    assert read_rows(sql_catalog) == [{'id': 1, 'v': 'a'}, {'id': 2, 'v': None}]


def test_compatible_types_are_cast_to_table_schema(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, APPEND)
    load_data_to_iceberg(pa.table({'id': pa.array([2], pa.int32())}), TID, sql_catalog, APPEND)

    assert read_rows(sql_catalog) == [{'id': 1}, {'id': 2}]


def test_values_that_cannot_be_cast_become_nulls(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(pa.table({'id': [1], 'n': [10]}), TID, sql_catalog, APPEND)
    load_data_to_iceberg(pa.table({'id': [2], 'n': ['not a number']}), TID, sql_catalog, APPEND)

    assert read_rows(sql_catalog) == [{'id': 1, 'n': 10}, {'id': 2, 'n': None}]


def test_load_timestamp_column_is_added_without_schema_evolution(sql_catalog: SqlCatalog) -> None:
    load_ts = datetime(2025, 1, 1, 12, 0, 0)
    load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, APPEND)
    config = LoaderConfig(write_mode='append', load_timestamp=load_ts)

    load_data_to_iceberg(pa.table({'id': [2]}), TID, sql_catalog, config)

    assert read_rows(sql_catalog) == [{'id': 1, '_load_dttm': None}, {'id': 2, '_load_dttm': load_ts}]


def test_partition_on_load_timestamp(sql_catalog: SqlCatalog) -> None:
    config = LoaderConfig(
        write_mode='append',
        load_timestamp=datetime(2025, 1, 1),
        partition_col='day(_load_dttm)',
    )

    result = load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, config)

    assert [f.name for f in sql_catalog.load_table(TID).spec().fields] == ['_load_dttm_day']
    assert result['partition_col'] == 'day(_load_dttm)'


def test_string_column_partitioned_by_time_is_promoted_to_timestamp(sql_catalog: SqlCatalog) -> None:
    data = pa.table({'id': [1], 'd': ['2024-01-02 00:00:00']})

    load_data_to_iceberg(data, TID, sql_catalog, LoaderConfig(write_mode='append', partition_col='day(d)'))

    assert [f.name for f in sql_catalog.load_table(TID).spec().fields] == ['d_day']
    assert read_rows(sql_catalog) == [{'id': 1, 'd': datetime(2024, 1, 2)}]


def test_table_properties_merge_with_defaults(sql_catalog: SqlCatalog) -> None:
    config = LoaderConfig(write_mode='append', table_properties={'custom.prop': 'value'})

    load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, config)

    properties = sql_catalog.load_table(TID).properties
    assert properties['custom.prop'] == 'value'
    assert properties['write.parquet.compression-codec'] == 'zstd'


def test_table_properties_do_not_leak_between_loads(sql_catalog: SqlCatalog) -> None:
    loader = IcebergLoader(sql_catalog)

    loader.load_data(pa.table({'id': [1]}), ('default', 'first'), LoaderConfig(table_properties={'prop1': '1'}))
    loader.load_data(pa.table({'id': [1]}), ('default', 'second'), LoaderConfig(table_properties={'prop2': '2'}))

    second = sql_catalog.load_table(('default', 'second')).properties
    assert second['prop2'] == '2'
    assert 'prop1' not in second


def test_commit_interval_groups_batches_into_one_commit(sql_catalog: SqlCatalog) -> None:
    batches = [batch(id=[1]), batch(id=[2]), batch(id=[3])]
    config = LoaderConfig(write_mode='append', commit_interval=10)

    result = load_batches_to_iceberg(iter(batches), TID, sql_catalog, config)

    assert len(list(sql_catalog.load_table(TID).snapshots())) == 1
    assert result['rows_loaded'] == 3
    assert result['batches_processed'] == 3


def test_empty_iterator_creates_no_table(sql_catalog: SqlCatalog) -> None:
    result = load_batches_to_iceberg(iter([]), TID, sql_catalog, APPEND)

    assert result['rows_loaded'] == 0
    assert result['batches_processed'] == 0
    assert result['table_location'] == 'none'
    assert result['snapshot_id'] == 'none'
    assert not sql_catalog.table_exists(TID)


def test_mixed_buffer_keeps_partition_on_load_timestamp(sql_catalog: SqlCatalog) -> None:
    batches = [batch(id=[1]), batch(id=[2], extra=['x'])]
    config = LoaderConfig(
        write_mode='append',
        schema_evolution=True,
        commit_interval=2,
        load_timestamp=datetime(2025, 1, 1),
        partition_col='day(_load_dttm)',
    )

    load_batches_to_iceberg(iter(batches), TID, sql_catalog, config)

    assert [f.name for f in sql_catalog.load_table(TID).spec().fields] == ['_load_dttm_day']


def test_new_table_column_order_puts_load_timestamp_before_evolved_columns(sql_catalog: SqlCatalog) -> None:
    batches = [batch(id=[1]), batch(id=[2], x=['a'])]
    config = LoaderConfig(
        write_mode='append',
        schema_evolution=True,
        commit_interval=2,
        load_timestamp=datetime(2025, 1, 1),
    )

    load_batches_to_iceberg(iter(batches), TID, sql_catalog, config)

    assert sql_catalog.load_table(TID).schema().column_names == ['id', '_load_dttm', 'x']


def test_reused_loader_keeps_loads_separate(sql_catalog: SqlCatalog) -> None:
    loader = IcebergLoader(sql_catalog)

    loader.load_data(pa.table({'id': [1]}), ('default', 'first'), APPEND)
    second = loader.load_data(pa.table({'id': [2]}), ('default', 'second'), APPEND)

    assert read_rows(sql_catalog, ('default', 'first')) == [{'id': 1}]
    assert read_rows(sql_catalog, ('default', 'second')) == [{'id': 2}]
    assert second['new_table_created'] is True
