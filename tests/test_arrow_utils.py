import unittest

import pyarrow as pa
from iceberg_loader.arrow_utils import (
    convert_column_type,
    convert_table_types,
    create_arrow_table_from_data,
    create_record_batches_from_dicts,
)


class TestArrowUtils(unittest.TestCase):
    def test_create_arrow_table_basic(self):
        data = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
        table = create_arrow_table_from_data(data)

        self.assertEqual(table.num_rows, 2)
        self.assertEqual(set(table.schema.names), {'id', 'name'})

    def test_create_arrow_table_empty(self):
        data = []
        table = create_arrow_table_from_data(data)

        self.assertEqual(table.num_rows, 0)
        self.assertEqual(len(table.schema), 0)

    def test_create_arrow_table_serializes_complex(self):
        data = [
            {'id': 1, 'complex_field': {'a': 1, 'b': 'x'}},
            {'id': 2, 'complex_field': [1, 2, 3]},
        ]

        table = create_arrow_table_from_data(data)

        self.assertEqual(table.schema.field('complex_field').type, pa.string())
        self.assertEqual(
            table.to_pydict()['complex_field'],
            ['{"a":1,"b":"x"}', '[1,2,3]'],
        )

    def test_create_record_batches_from_dicts(self):
        data = [{'id': i, 'value': f'v{i}'} for i in range(25)]
        batches = list(create_record_batches_from_dicts(iter(data), batch_size=10))

        self.assertEqual(len(batches), 3)
        self.assertEqual(batches[0].num_rows, 10)
        self.assertEqual(batches[1].num_rows, 10)
        self.assertEqual(batches[2].num_rows, 5)

    def test_create_record_batches_from_dicts_empty(self):
        data = []
        batches = list(create_record_batches_from_dicts(iter(data), batch_size=10))

        self.assertEqual(len(batches), 0)

    def test_convert_column_type_warns_and_nulls(self):
        column = pa.array(['a', 'b'])
        with self.assertLogs('iceberg_loader.arrow_utils', level='WARNING') as cm:
            converted = convert_column_type(column, pa.int64(), 'bad')
        self.assertEqual(converted.to_pylist(), [None, None])
        self.assertTrue(any('bad' in msg for msg in cm.output))

    def test_convert_table_types_missing_column(self):
        table = pa.Table.from_pydict({'a': [1, 2]})
        target = pa.schema([pa.field('a', pa.int64()), pa.field('b', pa.string())])
        converted = convert_table_types(table, target)
        self.assertEqual(converted.column('b').to_pylist(), [None, None])

    def test_convert_table_types_cast_error_branch(self):
        table = pa.Table.from_pydict({'a': ['x', 'y']})
        target = pa.schema([pa.field('a', pa.int64())])
        with self.assertLogs('iceberg_loader.arrow_utils', level='WARNING'):
            converted = convert_table_types(table, target)
        self.assertEqual(converted.column('a').to_pylist(), [None, None])


if __name__ == '__main__':
    unittest.main()
