import unittest
from datetime import date
from unittest.mock import MagicMock, patch

import pyarrow as pa
from iceberg_loader.iceberg_loader import IcebergLoader, load_data_to_iceberg
from iceberg_loader.settings import TABLE_PROPERTIES
from pyiceberg.exceptions import NoSuchTableError


class TestIcebergLoader(unittest.TestCase):
    def setUp(self):
        self.mock_catalog = MagicMock()
        self.loader = IcebergLoader(self.mock_catalog)
        self.table_identifier = ('default', 'test_table')
        self.arrow_schema = pa.schema(
            [pa.field('id', pa.int64()), pa.field('name', pa.string()), pa.field('date_col', pa.date32())]
        )
        self.arrow_table = pa.Table.from_pydict(
            {'id': [1, 2], 'name': ['a', 'b'], 'date_col': [date(2023, 1, 1), date(2023, 1, 2)]},
            schema=self.arrow_schema,
        )

    def test_init_default_properties(self):
        self.assertEqual(self.loader.table_properties, TABLE_PROPERTIES)

    def test_init_custom_properties(self):
        custom_props = {'write.format.default': 'orc', 'new.prop': 'value'}
        loader = IcebergLoader(self.mock_catalog, table_properties=custom_props)
        expected_props = TABLE_PROPERTIES.copy()
        expected_props.update(custom_props)
        self.assertEqual(loader.table_properties, expected_props)

    def test_load_data_create_table(self):
        mock_table = MagicMock()
        self.mock_catalog.load_table.side_effect = [NoSuchTableError, mock_table]
        # Use schema manager directly for expectations
        expected_iceberg_schema = self.loader.schema_manager._arrow_to_iceberg(self.arrow_schema)
        mock_table.schema.return_value = expected_iceberg_schema
        # Ensure ensure_table_exists returns the mock table
        # We need to mock schema_manager.ensure_table_exists or let it run
        # Since we use real SchemaManager, we need to mock catalog.create_table behavior

        result = self.loader.load_data(self.arrow_table, self.table_identifier, partition_col='date_col')

        self.mock_catalog.create_table.assert_called_once()
        self.assertEqual(result['rows_loaded'], 2)
        self.assertEqual(result['partition_col'], 'date_col')

    def test_load_data_append_existing(self):
        mock_table = MagicMock()
        self.mock_catalog.load_table.return_value = mock_table
        expected_iceberg_schema = self.loader.schema_manager._arrow_to_iceberg(self.arrow_schema)
        mock_table.schema.return_value = expected_iceberg_schema

        self.loader.load_data(self.arrow_table, self.table_identifier, write_mode='append')

        self.mock_catalog.create_table.assert_not_called()
        mock_table.transaction.assert_called_once()
        mock_table.transaction.return_value.__enter__.return_value.append.assert_called()

    def test_load_data_overwrite_existing(self):
        mock_table = MagicMock()
        self.mock_catalog.load_table.return_value = mock_table
        expected_iceberg_schema = self.loader.schema_manager._arrow_to_iceberg(self.arrow_schema)
        mock_table.schema.return_value = expected_iceberg_schema

        self.loader.load_data(self.arrow_table, self.table_identifier, write_mode='overwrite')

        # Overwrite strategy uses overwrite
        mock_table.transaction.return_value.__enter__.return_value.overwrite.assert_called()

    def test_load_data_append_replace_filter(self):
        mock_table = MagicMock()
        self.mock_catalog.load_table.return_value = mock_table
        expected_iceberg_schema = self.loader.schema_manager._arrow_to_iceberg(self.arrow_schema)
        mock_table.schema.return_value = expected_iceberg_schema

        txn = mock_table.transaction.return_value.__enter__.return_value
        self.loader.load_data(
            self.arrow_table, self.table_identifier, write_mode='append', replace_filter="date_col == '2023-01-01'"
        )

        txn.delete.assert_called_once_with("date_col == '2023-01-01'")
        txn.append.assert_called_once()

    def test_load_data_upsert(self):
        mock_table = MagicMock()
        self.mock_catalog.load_table.return_value = mock_table
        expected_iceberg_schema = self.loader.schema_manager._arrow_to_iceberg(self.arrow_schema)
        mock_table.schema.return_value = expected_iceberg_schema

        self.loader.load_data(self.arrow_table, self.table_identifier, write_mode='upsert', join_cols=['id'])

        mock_table.upsert.assert_called_once()
        call_args = mock_table.upsert.call_args
        self.assertEqual(call_args.kwargs['join_cols'], ['id'])
        mock_table.transaction.assert_not_called()

    def test_public_api_wrapper(self):
        with patch('iceberg_loader.iceberg_loader.IcebergLoader') as mock_loader_cls:
            mock_instance = mock_loader_cls.return_value
            mock_instance.load_data.return_value = {'status': 'ok'}

            load_data_to_iceberg(self.arrow_table, self.table_identifier, self.mock_catalog)

            mock_loader_cls.assert_called_with(self.mock_catalog, None)
            mock_instance.load_data.assert_called_once()

    def test_field_ids_preserved_on_evolution(self):
        base_schema = self.loader.schema_manager._arrow_to_iceberg(self.arrow_schema)
        extended_arrow = pa.schema(
            [
                pa.field('id', pa.int64()),
                pa.field('name', pa.string()),
                pa.field('date_col', pa.date32()),
                pa.field('extra', pa.string()),
            ]
        )
        evolved = self.loader.schema_manager._arrow_to_iceberg(extended_arrow, existing_schema=base_schema)
        ids = {f.name: f.field_id for f in evolved.fields}
        self.assertEqual(ids['id'], base_schema.find_field('id').field_id)
        self.assertEqual(ids['name'], base_schema.find_field('name').field_id)
        self.assertEqual(ids['date_col'], base_schema.find_field('date_col').field_id)
        self.assertGreater(ids['extra'], max(f.field_id for f in base_schema.fields))

    def test_stream_batches_schema_evolution_midstream(self):
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
            table_identifier=self.table_identifier,
            write_mode='append',
            schema_evolution=True,
        )

        # Verify data was successfully loaded despite schema evolution
        self.assertEqual(result['rows_loaded'], 2)
        self.assertEqual(result['batches_processed'], 2)

    def test_load_data_batches_empty_iterator(self):
        result = self.loader.load_data_batches(
            batch_iterator=iter([]), table_identifier=self.table_identifier, write_mode='append'
        )
        self.assertEqual(result['rows_loaded'], 0)
        self.assertEqual(result['batches_processed'], 0)

    def test_overwrite_branch_append_path(self):
        mock_table = MagicMock()
        self.mock_catalog.load_table.return_value = mock_table
        expected_iceberg_schema = self.loader.schema_manager._arrow_to_iceberg(self.arrow_schema)
        mock_table.schema.return_value = expected_iceberg_schema

        txn = mock_table.transaction.return_value.__enter__.return_value
        self.loader.load_data(self.arrow_table, self.table_identifier, write_mode='append', replace_filter=None)
        txn.overwrite.assert_not_called()
        txn.append.assert_called()


if __name__ == '__main__':
    unittest.main()
