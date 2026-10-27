import json
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from pyiceberg.catalog.sql import SqlCatalog

from iceberg_loader import LoaderConfig, load_data_to_iceberg
from iceberg_loader.core.config import TABLE_PROPERTIES
from iceberg_loader.core.conform import BatchConformer
from iceberg_loader.core.schema import SchemaManager

TID = ('default', 'events')


def make_conformer(catalog: SqlCatalog, config: LoaderConfig) -> BatchConformer:
    properties = TABLE_PROPERTIES.copy()
    return BatchConformer(SchemaManager(catalog, properties), TID, config, properties)


def batch(**columns: list[Any]) -> pa.RecordBatch:
    return pa.RecordBatch.from_pydict(columns)


def test_first_conform_creates_table_and_marks_it_new(sql_catalog: SqlCatalog) -> None:
    conformer = make_conformer(sql_catalog, LoaderConfig(write_mode='append'))
    assert conformer.table is None

    data = conformer.conform([batch(id=[1, 2])])

    assert sql_catalog.table_exists(TID)
    assert conformer.table is not None
    assert conformer.new_table_created is True
    assert data.to_pylist() == [{'id': 1}, {'id': 2}]


def test_existing_table_with_data_is_not_marked_new(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, LoaderConfig(write_mode='append'))
    conformer = make_conformer(sql_catalog, LoaderConfig(write_mode='append'))

    conformer.conform([batch(id=[2])])

    assert conformer.new_table_created is False


def test_existing_empty_table_is_not_marked_new(sql_catalog: SqlCatalog) -> None:
    sql_catalog.create_table(TID, schema=pa.schema([('id', pa.int64())]))
    conformer = make_conformer(sql_catalog, LoaderConfig(write_mode='append'))

    conformer.conform([batch(id=[1])])

    assert conformer.new_table_created is False


def test_output_matches_table_schema(sql_catalog: SqlCatalog) -> None:
    load_data_to_iceberg(pa.table({'id': [1], 'v': ['a']}), TID, sql_catalog, LoaderConfig(write_mode='append'))
    conformer = make_conformer(sql_catalog, LoaderConfig(write_mode='append'))

    data = conformer.conform([batch(id=pa.array([2], pa.int32()), zzz=['dropped'])])

    assert data.schema.names == ['id', 'v']
    assert data.schema.field('id').type == pa.int64()
    assert data.to_pylist() == [{'id': 2, 'v': None}]


def test_mixed_schemas_with_evolution_add_columns(sql_catalog: SqlCatalog) -> None:
    conformer = make_conformer(sql_catalog, LoaderConfig(write_mode='append', schema_evolution=True))

    data = conformer.conform([batch(id=[1]), batch(id=[2], extra=['x'])])

    assert sql_catalog.load_table(TID).schema().column_names == ['id', 'extra']
    assert data.to_pylist() == [{'id': 1, 'extra': None}, {'id': 2, 'extra': 'x'}]


def test_mixed_schemas_without_evolution_raise_before_table_creation(sql_catalog: SqlCatalog) -> None:
    conformer = make_conformer(sql_catalog, LoaderConfig(write_mode='append'))

    with pytest.raises(pa.ArrowInvalid):
        conformer.conform([batch(id=[1]), batch(id=[2], extra=['x'])])

    assert not sql_catalog.table_exists(TID)
    assert conformer.table is None


def test_load_timestamp_exists_before_table_creation(sql_catalog: SqlCatalog) -> None:
    load_ts = datetime(2025, 1, 1)
    config = LoaderConfig(
        write_mode='append',
        schema_evolution=True,
        load_timestamp=load_ts,
        partition_col='day(_load_dttm)',
    )
    conformer = make_conformer(sql_catalog, config)

    data = conformer.conform([batch(id=[1]), batch(id=[2], extra=['x'])])

    assert [f.name for f in sql_catalog.load_table(TID).spec().fields] == ['_load_dttm_day']
    assert data.column('_load_dttm').to_pylist() == [load_ts, load_ts]


def test_second_conform_reuses_table(sql_catalog: SqlCatalog) -> None:
    conformer = make_conformer(sql_catalog, LoaderConfig(write_mode='append'))
    conformer.conform([batch(id=[1])])
    first_table = conformer.table

    conformer.conform([batch(id=[2])])

    assert conformer.table is first_table
    assert conformer.new_table_created is True


def test_empty_buffer_is_rejected(sql_catalog: SqlCatalog) -> None:
    conformer = make_conformer(sql_catalog, LoaderConfig(write_mode='append'))

    with pytest.raises(ValueError, match='at least one batch'):
        conformer.conform([])


def test_explicit_format_version_mismatch_warns(
    sql_catalog: SqlCatalog,
    loader_warnings: Callable[[], list[str]],
) -> None:
    v1 = LoaderConfig(write_mode='append', table_properties={'format-version': 1})
    load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, v1)

    v2 = LoaderConfig(write_mode='append', table_properties={'format-version': 2})
    load_data_to_iceberg(pa.table({'id': [2]}), TID, sql_catalog, v2)

    assert sql_catalog.load_table(TID).format_version == 1
    assert loader_warnings() == [
        'Table default.events has format-version 1, but table_properties request 2. '
        'format-version applies only when a table is created; the table keeps version 1.',
    ]


def test_default_format_version_does_not_warn_for_existing_table(
    sql_catalog: SqlCatalog,
    loader_warnings: Callable[[], list[str]],
) -> None:
    v1 = LoaderConfig(write_mode='append', table_properties={'format-version': 1})
    load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, v1)

    load_data_to_iceberg(pa.table({'id': [2]}), TID, sql_catalog, LoaderConfig(write_mode='append'))

    assert loader_warnings() == []


def test_matching_format_version_does_not_warn(
    sql_catalog: SqlCatalog,
    loader_warnings: Callable[[], list[str]],
) -> None:
    v2 = LoaderConfig(write_mode='append', table_properties={'format-version': '2'})
    load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, LoaderConfig(write_mode='append'))

    load_data_to_iceberg(pa.table({'id': [2]}), TID, sql_catalog, v2)

    assert loader_warnings() == []


@pytest.mark.parametrize('bad_version', ['v2', 2.9, True])
def test_non_integer_format_version_for_existing_table_warns_and_loads(
    sql_catalog: SqlCatalog,
    loader_warnings: Callable[[], list[str]],
    bad_version: Any,
) -> None:
    load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, LoaderConfig(write_mode='append'))
    bad = LoaderConfig(write_mode='append', table_properties={'format-version': bad_version})

    load_data_to_iceberg(pa.table({'id': [2]}), TID, sql_catalog, bad)

    assert sql_catalog.load_table(TID).scan().to_arrow().num_rows == 2
    assert loader_warnings() == [f'Ignoring non-integer format-version {bad_version!r} for existing table.']


@pytest.mark.parametrize('version', [3, '3'])
def test_new_table_with_unwritable_format_version_fails_fast(sql_catalog: SqlCatalog, version: Any) -> None:
    v3 = LoaderConfig(write_mode='append', table_properties={'format-version': version})

    with pytest.raises(NotImplementedError, match='uses Iceberg format-version 3, but the installed PyIceberg'):
        load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, v3)

    assert not sql_catalog.table_exists(TID)


def register_v3_table(catalog: SqlCatalog) -> None:
    """Registers a v3 table as another engine would leave it; PyIceberg cannot create one itself."""
    seed = catalog.create_table(('default', 'v2_seed'), schema=pa.schema([pa.field('id', pa.int64())]))
    metadata_path = Path(seed.metadata_location.removeprefix('file://'))
    metadata = json.loads(metadata_path.read_text())
    metadata['format-version'] = 3
    metadata['next-row-id'] = 0
    v3_path = metadata_path.with_name('00000-v3.metadata.json')
    v3_path.write_text(json.dumps(metadata))
    catalog.drop_table(('default', 'v2_seed'))
    catalog.register_table(TID, f'file://{v3_path}')


@pytest.mark.parametrize(
    'config',
    [
        LoaderConfig(write_mode='append'),
        LoaderConfig(write_mode='overwrite'),
        LoaderConfig(write_mode='append', replace_filter='id = 1'),
        LoaderConfig(write_mode='upsert', join_cols=['id']),
    ],
    ids=['append', 'overwrite', 'idempotent', 'upsert'],
)
def test_existing_v3_table_fails_fast(sql_catalog: SqlCatalog, config: LoaderConfig) -> None:
    register_v3_table(sql_catalog)

    with pytest.raises(NotImplementedError, match=r'default\.events uses Iceberg format-version 3'):
        load_data_to_iceberg(pa.table({'id': [1]}), TID, sql_catalog, config)

    assert sql_catalog.load_table(TID).current_snapshot() is None


def test_nanosecond_timestamps_are_stored_as_microseconds_with_warning(
    sql_catalog: SqlCatalog,
    loader_warnings: Callable[[], list[str]],
) -> None:
    ts = pa.array([1_700_000_000_123_456_789], type=pa.timestamp('ns'))

    load_data_to_iceberg(pa.table({'ts': ts}), TID, sql_catalog, LoaderConfig(write_mode='append'))

    stored = sql_catalog.load_table(TID).scan().to_arrow().column('ts')
    assert stored.type == pa.timestamp('us')
    assert stored.cast(pa.int64()).to_pylist() == [1_700_000_000_123_456]
    assert loader_warnings() == [
        'Timestamp precision lost for column ts (timestamp[ns] -> timestamp[us]). Values were truncated.',
    ]
