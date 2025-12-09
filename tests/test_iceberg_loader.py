import unittest
from datetime import date
from unittest.mock import MagicMock, patch

import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError

from iceberg_loader.iceberg_loader import IcebergLoader, load_data_to_iceberg
from iceberg_loader.settings import TABLE_PROPERTIES


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
        expected_iceberg_schema = self.loader._convert_arrow_to_iceberg_schema(self.arrow_schema)
        mock_table.schema.return_value = expected_iceberg_schema

        result = self.loader.load_data(self.arrow_table, self.table_identifier, partition_col='date_col')

        self.mock_catalog.create_table.assert_called_once()
        self.assertEqual(result['rows_loaded'], 2)
        self.assertEqual(result['partition_col'], 'date_col')

    def test_load_data_append_existing(self):
        mock_table = MagicMock()
        self.mock_catalog.load_table.return_value = mock_table
        # Return a valid Iceberg schema that matches arrow_table
        # Use the internal helper to generate it from the arrow schema for convenience
        expected_iceberg_schema = self.loader._convert_arrow_to_iceberg_schema(self.arrow_schema)
        mock_table.schema.return_value = expected_iceberg_schema

        self.loader.load_data(self.arrow_table, self.table_identifier, write_mode='append')

        self.mock_catalog.create_table.assert_not_called()
        mock_table.transaction.assert_called_once()
        mock_table.transaction.return_value.__enter__.return_value.append.assert_called()

    def test_load_data_overwrite_existing(self):
        mock_table = MagicMock()
        self.mock_catalog.load_table.return_value = mock_table
        # Return a valid Iceberg schema that matches arrow_table
        expected_iceberg_schema = self.loader._convert_arrow_to_iceberg_schema(self.arrow_schema)
        mock_table.schema.return_value = expected_iceberg_schema

        self.loader.load_data(self.arrow_table, self.table_identifier, write_mode='overwrite')

        mock_table.transaction.return_value.__enter__.return_value.overwrite.assert_called()

    def test_public_api_wrapper(self):
        with patch('iceberg_loader.iceberg_loader.IcebergLoader') as mock_loader_cls:
            mock_instance = mock_loader_cls.return_value
            mock_instance.load_data.return_value = {'status': 'ok'}

            load_data_to_iceberg(self.arrow_table, self.table_identifier, self.mock_catalog)

            mock_loader_cls.assert_called_with(self.mock_catalog, None)
            mock_instance.load_data.assert_called_once()


if __name__ == '__main__':
    unittest.main()
