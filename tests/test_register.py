from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from iceberg_loader import LoaderConfig, load_data_to_iceberg, register_parquet_files

TID = ('default', 'events')
APPEND = LoaderConfig(write_mode='append')
DAY_ONE_US = 1_704_067_200_000_000
DAY_TWO_US = 1_704_153_600_000_000


def seed_table(catalog: SqlCatalog) -> None:
    data = pa.table({'id': pa.array([0], type=pa.int64()), 'name': pa.array(['seed'], type=pa.string())})
    load_data_to_iceberg(data, TID, catalog, APPEND)


def write_parquet(tmp_path: Path, name: str, table: pa.Table) -> str:
    path = tmp_path / name
    pq.write_table(table, path)
    return str(path)


def read_rows(catalog: SqlCatalog) -> list[dict[str, Any]]:
    return catalog.load_table(TID).scan().to_arrow().sort_by('id').to_pylist()


def snapshot_ids(catalog: SqlCatalog) -> list[int]:
    return [snapshot.snapshot_id for snapshot in catalog.load_table(TID).snapshots()]


def people(ids: list[int], names: list[str]) -> pa.Table:
    return pa.table({'id': pa.array(ids, type=pa.int64()), 'name': pa.array(names, type=pa.string())})


def timestamp_us(epoch_us: int) -> Any:
    return pa.scalar(epoch_us, pa.timestamp('us')).as_py()


def events(ids: list[int], timestamps_us: list[int]) -> pa.Table:
    return pa.table(
        {
            'id': pa.array(ids, type=pa.int64()),
            'ts': pa.array([timestamp_us(value) for value in timestamps_us], type=pa.timestamp('us')),
        },
    )


def test_registers_existing_files_without_rewriting(sql_catalog: SqlCatalog, tmp_path: Path) -> None:
    seed_table(sql_catalog)
    table = sql_catalog.load_table(TID)
    files_before = table.inspect.files().num_rows
    snapshots_before = len(table.snapshots())
    paths = [
        write_parquet(tmp_path, 'first.parquet', people([1, 2], ['a', 'b'])),
        write_parquet(tmp_path, 'second.parquet', people([3], ['c'])),
    ]

    result = register_parquet_files(paths, TID, sql_catalog)

    table = sql_catalog.load_table(TID)
    snapshot = table.current_snapshot()
    assert snapshot is not None
    assert result == {
        'files_added': 2,
        'rows_added': 3,
        'snapshot_id': snapshot.snapshot_id,
        'table_location': table.location(),
    }
    assert read_rows(sql_catalog) == [
        {'id': 0, 'name': 'seed'},
        {'id': 1, 'name': 'a'},
        {'id': 2, 'name': 'b'},
        {'id': 3, 'name': 'c'},
    ]
    assert table.inspect.files().num_rows == files_before + 2
    assert len(table.snapshots()) == snapshots_before + 1
    assert set(paths) <= set(table.inspect.files().column('file_path').to_pylist())
    assert all(Path(path).exists() for path in paths)


def test_duplicate_file_is_rejected_by_default(sql_catalog: SqlCatalog, tmp_path: Path) -> None:
    seed_table(sql_catalog)
    path = write_parquet(tmp_path, 'dupe.parquet', people([1, 2], ['a', 'b']))
    register_parquet_files([path], TID, sql_catalog)
    snapshots_before = snapshot_ids(sql_catalog)
    rows_before = read_rows(sql_catalog)

    with pytest.raises(ValueError, match='already referenced'):
        register_parquet_files([path], TID, sql_catalog)

    assert snapshot_ids(sql_catalog) == snapshots_before
    assert read_rows(sql_catalog) == rows_before


def test_disabling_duplicate_check_registers_the_file_twice(
    sql_catalog: SqlCatalog,
    tmp_path: Path,
    loader_warnings: Callable[[], list[str]],
) -> None:
    seed_table(sql_catalog)
    path = write_parquet(tmp_path, 'dupe.parquet', people([1], ['a']))
    register_parquet_files([path], TID, sql_catalog)

    result = register_parquet_files([path], TID, sql_catalog, check_duplicate_files=False)

    assert result['rows_added'] == 1
    assert [row['id'] for row in read_rows(sql_catalog)] == [0, 1, 1]
    assert any('may be added twice' in message for message in loader_warnings())


def test_duplicate_paths_in_one_call_are_rejected(sql_catalog: SqlCatalog, tmp_path: Path) -> None:
    seed_table(sql_catalog)
    path = write_parquet(tmp_path, 'dupe.parquet', people([1], ['a']))

    with pytest.raises(ValueError, match='unique'):
        register_parquet_files([path, path], TID, sql_catalog)


def test_unknown_file_column_raises_without_creating_snapshot(sql_catalog: SqlCatalog, tmp_path: Path) -> None:
    seed_table(sql_catalog)
    path = write_parquet(
        tmp_path,
        'extra.parquet',
        pa.table(
            {
                'id': pa.array([1], type=pa.int64()),
                'name': pa.array(['a'], type=pa.string()),
                'extra': pa.array(['x'], type=pa.string()),
            },
        ),
    )
    snapshots_before = snapshot_ids(sql_catalog)

    with pytest.raises(ValueError) as exc_info:
        register_parquet_files([path], TID, sql_catalog)

    message = str(exc_info.value)
    assert path in message
    assert 'extra' in message
    assert snapshot_ids(sql_catalog) == snapshots_before


def test_column_type_mismatch_raises_without_creating_snapshot(sql_catalog: SqlCatalog, tmp_path: Path) -> None:
    seed_table(sql_catalog)
    path = write_parquet(
        tmp_path,
        'wrong_type.parquet',
        pa.table({'id': pa.array(['1'], type=pa.string()), 'name': pa.array(['a'], type=pa.string())}),
    )
    snapshots_before = snapshot_ids(sql_catalog)

    with pytest.raises(ValueError) as exc_info:
        register_parquet_files([path], TID, sql_catalog)

    message = str(exc_info.value)
    assert path in message
    assert "'id'" in message
    assert snapshot_ids(sql_catalog) == snapshots_before


def test_promotable_column_type_is_accepted(sql_catalog: SqlCatalog, tmp_path: Path) -> None:
    seed_table(sql_catalog)
    path = write_parquet(
        tmp_path,
        'promotable.parquet',
        pa.table({'id': pa.array([1, 2], type=pa.int32()), 'name': pa.array(['a', 'b'], type=pa.string())}),
    )

    result = register_parquet_files([path], TID, sql_catalog)

    assert result['rows_added'] == 2
    assert read_rows(sql_catalog) == [
        {'id': 0, 'name': 'seed'},
        {'id': 1, 'name': 'a'},
        {'id': 2, 'name': 'b'},
    ]


def test_missing_file_raises_without_creating_snapshot(sql_catalog: SqlCatalog, tmp_path: Path) -> None:
    seed_table(sql_catalog)
    snapshots_before = snapshot_ids(sql_catalog)

    with pytest.raises(FileNotFoundError):
        register_parquet_files([str(tmp_path / 'missing.parquet')], TID, sql_catalog)

    assert snapshot_ids(sql_catalog) == snapshots_before


def test_snapshot_properties_are_recorded(sql_catalog: SqlCatalog, tmp_path: Path) -> None:
    seed_table(sql_catalog)
    path = write_parquet(tmp_path, 'props.parquet', people([1], ['a']))

    register_parquet_files([path], TID, sql_catalog, snapshot_properties={'source': 'legacy-export'})

    snapshot = sql_catalog.load_table(TID).current_snapshot()
    assert snapshot is not None
    assert snapshot.summary is not None
    assert snapshot.summary['source'] == 'legacy-export'


def test_missing_required_column_raises_without_creating_snapshot(sql_catalog: SqlCatalog, tmp_path: Path) -> None:
    sql_catalog.create_table(
        TID,
        Schema(
            NestedField(1, 'id', LongType(), required=True),
            NestedField(2, 'name', StringType(), required=False),
        ),
    )
    path = write_parquet(tmp_path, 'no_id.parquet', pa.table({'name': pa.array(['a'], type=pa.string())}))
    snapshots_before = snapshot_ids(sql_catalog)

    with pytest.raises(ValueError) as exc_info:
        register_parquet_files([path], TID, sql_catalog)

    message = str(exc_info.value)
    assert path in message
    assert 'id' in message
    assert snapshot_ids(sql_catalog) == snapshots_before


def test_missing_nullable_column_is_read_as_null(sql_catalog: SqlCatalog, tmp_path: Path) -> None:
    seed_table(sql_catalog)
    path = write_parquet(
        tmp_path,
        'no_name.parquet',
        pa.table({'id': pa.array([1, 2], type=pa.int64())}),
    )

    result = register_parquet_files([path], TID, sql_catalog)

    assert result['rows_added'] == 2
    assert read_rows(sql_catalog) == [
        {'id': 0, 'name': 'seed'},
        {'id': 1, 'name': None},
        {'id': 2, 'name': None},
    ]


def test_empty_file_paths_raises(sql_catalog: SqlCatalog) -> None:
    with pytest.raises(ValueError, match='cannot be empty'):
        register_parquet_files([], TID, sql_catalog)


def test_missing_table_raises_with_its_identifier(sql_catalog: SqlCatalog, tmp_path: Path) -> None:
    path = write_parquet(tmp_path, 'orphan.parquet', people([1], ['a']))

    with pytest.raises(NoSuchTableError, match=r'default\.absent'):
        register_parquet_files([path], ('default', 'absent'), sql_catalog)


def create_day_partitioned_table(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(
        events([0], [DAY_ONE_US]),
        TID,
        sql_catalog,
        LoaderConfig(write_mode='append', partition_col='day(ts)'),
    )


def test_partitioned_table_registers_file_within_one_partition(sql_catalog: SqlCatalog, tmp_path: Path) -> None:
    create_day_partitioned_table(sql_catalog)
    day_two = timestamp_us(DAY_TWO_US)
    path = write_parquet(tmp_path, 'day-two.parquet', events([1], [DAY_TWO_US]))

    register_parquet_files([path], TID, sql_catalog)

    table = sql_catalog.load_table(TID)
    assert table.scan(row_filter=EqualTo(term=Reference('ts'), value=day_two)).to_arrow().sort_by('id').to_pylist() == [
        {'id': 1, 'ts': day_two},
    ]


def test_partitioned_table_rejects_file_spanning_partitions(sql_catalog: SqlCatalog, tmp_path: Path) -> None:
    create_day_partitioned_table(sql_catalog)
    path = write_parquet(tmp_path, 'spanning.parquet', events([1, 2], [DAY_ONE_US, DAY_TWO_US]))
    snapshots_before = snapshot_ids(sql_catalog)

    with pytest.raises(ValueError, match='more than one partition'):
        register_parquet_files([path], TID, sql_catalog)

    assert snapshot_ids(sql_catalog) == snapshots_before
