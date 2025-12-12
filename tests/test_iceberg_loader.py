from datetime import date
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest
from pyiceberg.exceptions import NoSuchTableError

from iceberg_loader.core.config import TABLE_PROPERTIES
from iceberg_loader.iceberg_loader import IcebergLoader, load_data_to_iceberg


@pytest.fixture()
def mock_catalog() -> MagicMock:
    return MagicMock()


@pytest.fixture()
def table_identifier() -> tuple[str, str]:
    return ('default', 'test_table')


@pytest.fixture()
def arrow_schema() -> pa.Schema:
    return pa.schema([pa.field('id', pa.int64()), pa.field('name', pa.string()), pa.field('date_col', pa.date32())])


@pytest.fixture()
def arrow_table(arrow_schema: pa.Schema) -> pa.Table:
    return pa.Table.from_pydict(
        {'id': [1, 2], 'name': ['a', 'b'], 'date_col': [date(2023, 1, 1), date(2023, 1, 2)]},
        schema=arrow_schema,
    )


@pytest.fixture()
def loader(mock_catalog: MagicMock) -> IcebergLoader:
    return IcebergLoader(mock_catalog)


def test_init_default_properties(loader: IcebergLoader) -> None:
    assert loader.table_properties == TABLE_PROPERTIES


def test_init_custom_properties(mock_catalog: MagicMock) -> None:
    custom_props = {'write.format.default': 'orc', 'new.prop': 'value'}
    loader = IcebergLoader(mock_catalog, table_properties=custom_props)
    expected_props = TABLE_PROPERTIES.copy()
    expected_props.update(custom_props)
    assert loader.table_properties == expected_props


def test_load_data_create_table(
    loader: IcebergLoader, mock_catalog: MagicMock, arrow_table: pa.Table, table_identifier: tuple[str, str]
) -> None:
    mock_table = MagicMock()
    mock_catalog.load_table.side_effect = [NoSuchTableError, mock_table]
    expected_iceberg_schema = loader.schema_manager._arrow_to_iceberg(arrow_table.schema)
    mock_table.schema.return_value = expected_iceberg_schema

    result = loader.load_data(arrow_table, table_identifier, partition_col='date_col')

    mock_catalog.create_table.assert_called_once()
    assert result['rows_loaded'] == 2
    assert result['partition_col'] == 'date_col'


def test_load_data_append_existing(
    loader: IcebergLoader, mock_catalog: MagicMock, arrow_table: pa.Table, table_identifier: tuple[str, str]
) -> None:
    mock_table = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    expected_iceberg_schema = loader.schema_manager._arrow_to_iceberg(arrow_table.schema)
    mock_table.schema.return_value = expected_iceberg_schema

    loader.load_data(arrow_table, table_identifier, write_mode='append')

    mock_catalog.create_table.assert_not_called()
    mock_table.transaction.assert_called_once()
    mock_table.transaction.return_value.__enter__.return_value.append.assert_called()


def test_load_data_overwrite_existing(
    loader: IcebergLoader, mock_catalog: MagicMock, arrow_table: pa.Table, table_identifier: tuple[str, str]
) -> None:
    mock_table = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    expected_iceberg_schema = loader.schema_manager._arrow_to_iceberg(arrow_table.schema)
    mock_table.schema.return_value = expected_iceberg_schema

    loader.load_data(arrow_table, table_identifier, write_mode='overwrite')

    mock_table.transaction.return_value.__enter__.return_value.overwrite.assert_called()


def test_load_data_append_replace_filter(
    loader: IcebergLoader, mock_catalog: MagicMock, arrow_table: pa.Table, table_identifier: tuple[str, str]
) -> None:
    mock_table = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    expected_iceberg_schema = loader.schema_manager._arrow_to_iceberg(arrow_table.schema)
    mock_table.schema.return_value = expected_iceberg_schema

    txn = mock_table.transaction.return_value.__enter__.return_value
    loader.load_data(arrow_table, table_identifier, write_mode='append', replace_filter="date_col == '2023-01-01'")

    txn.delete.assert_called_once_with("date_col == '2023-01-01'")
    txn.append.assert_called_once()


def test_load_data_upsert(
    loader: IcebergLoader, mock_catalog: MagicMock, arrow_table: pa.Table, table_identifier: tuple[str, str]
) -> None:
    mock_table = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    expected_iceberg_schema = loader.schema_manager._arrow_to_iceberg(arrow_table.schema)
    mock_table.schema.return_value = expected_iceberg_schema

    loader.load_data(arrow_table, table_identifier, write_mode='upsert', join_cols=['id'])

    mock_table.upsert.assert_called_once()
    call_args = mock_table.upsert.call_args
    assert call_args.kwargs['join_cols'] == ['id']
    mock_table.transaction.assert_not_called()


def test_public_api_wrapper(arrow_table: pa.Table, table_identifier: tuple[str, str], mock_catalog: MagicMock) -> None:
    with patch('iceberg_loader.iceberg_loader.IcebergLoader') as mock_loader_cls:
        mock_instance = mock_loader_cls.return_value
        mock_instance.load_data.return_value = {'status': 'ok'}

        load_data_to_iceberg(arrow_table, table_identifier, mock_catalog)

        mock_loader_cls.assert_called_with(mock_catalog, None, default_config=None)
        mock_instance.load_data.assert_called_once()


def test_field_ids_preserved_on_evolution(loader: IcebergLoader, arrow_schema: pa.Schema) -> None:
    base_schema = loader.schema_manager._arrow_to_iceberg(arrow_schema)
    extended_arrow = pa.schema(
        [
            pa.field('id', pa.int64()),
            pa.field('name', pa.string()),
            pa.field('date_col', pa.date32()),
            pa.field('extra', pa.string()),
        ]
    )
    evolved = loader.schema_manager._arrow_to_iceberg(extended_arrow, existing_schema=base_schema)
    ids = {field.name: field.field_id for field in evolved.fields}
    assert ids['id'] == base_schema.find_field('id').field_id
    assert ids['name'] == base_schema.find_field('name').field_id
    assert ids['date_col'] == base_schema.find_field('date_col').field_id
    assert ids['extra'] > max(field.field_id for field in base_schema.fields)


def test_stream_batches_schema_evolution_midstream(table_identifier: tuple[str, str]) -> None:
    batch1 = pa.RecordBatch.from_pydict({'id': [1], 'value': ['a']})
    batch2 = pa.RecordBatch.from_pydict({'id': [2], 'value': ['b'], 'extra': ['x']})

    mock_table = MagicMock()
    mock_catalog = MagicMock()
    loader = IcebergLoader(mock_catalog)

    base_schema = loader.schema_manager._arrow_to_iceberg(batch1.schema)
    mock_catalog.load_table.return_value = mock_table
    mock_table.schema.return_value = base_schema
    mock_table.current_snapshot.return_value = MagicMock(snapshot_id=123)
    mock_table.location.return_value = 's3://test/path'

    result = loader.load_data_batches(
        batch_iterator=iter([batch1, batch2]),
        table_identifier=table_identifier,
        write_mode='append',
        schema_evolution=True,
    )

    assert result['rows_loaded'] == 2
    assert result['batches_processed'] == 2


def test_load_data_batches_empty_iterator(loader: IcebergLoader, table_identifier: tuple[str, str]) -> None:
    result = loader.load_data_batches(batch_iterator=iter([]), table_identifier=table_identifier, write_mode='append')
    assert result['rows_loaded'] == 0
    assert result['batches_processed'] == 0


def test_overwrite_branch_append_path(
    loader: IcebergLoader, mock_catalog: MagicMock, arrow_table: pa.Table, table_identifier: tuple[str, str]
) -> None:
    mock_table = MagicMock()
    mock_catalog.load_table.return_value = mock_table
    expected_iceberg_schema = loader.schema_manager._arrow_to_iceberg(arrow_table.schema)
    mock_table.schema.return_value = expected_iceberg_schema

    txn = mock_table.transaction.return_value.__enter__.return_value
    loader.load_data(arrow_table, table_identifier, write_mode='append', replace_filter=None)
    txn.overwrite.assert_not_called()
    txn.append.assert_called()
