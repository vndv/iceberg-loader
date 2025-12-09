import unittest

import pyarrow as pa

from iceberg_loader.arrow_utils import create_arrow_table_from_data, create_record_batches_from_dicts


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


if __name__ == '__main__':
    unittest.main()
