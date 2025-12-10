import unittest
from unittest.mock import MagicMock

import pyarrow as pa
from iceberg_loader.schema import SchemaManager
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    YearTransform,
)


class TestSchemaManagerPartitioning(unittest.TestCase):
    def setUp(self):
        self.mock_catalog = MagicMock()
        self.schema_manager = SchemaManager(self.mock_catalog, {})
        self.arrow_schema = pa.schema(
            [
                pa.field('id', pa.int64()),
                pa.field('ts', pa.timestamp('us')),
                pa.field('name', pa.string()),
            ]
        )
        self.iceberg_schema = self.schema_manager._arrow_to_iceberg(self.arrow_schema)

    def test_parse_partition_identity(self):
        spec = self.schema_manager._create_partition_spec(self.iceberg_schema, 'ts')
        self.assertIsNotNone(spec)
        self.assertIsInstance(spec, PartitionSpec)
        self.assertEqual(len(spec.fields), 1)
        self.assertIsInstance(spec.fields[0].transform, IdentityTransform)
        self.assertEqual(spec.fields[0].name, 'ts')

    def test_parse_partition_month(self):
        spec = self.schema_manager._create_partition_spec(self.iceberg_schema, 'month(ts)')
        self.assertIsNotNone(spec)
        self.assertIsInstance(spec.fields[0].transform, MonthTransform)
        self.assertEqual(spec.fields[0].name, 'ts_month')

    def test_parse_partition_day(self):
        spec = self.schema_manager._create_partition_spec(self.iceberg_schema, 'day(ts)')
        self.assertIsNotNone(spec)
        self.assertIsInstance(spec.fields[0].transform, DayTransform)
        self.assertEqual(spec.fields[0].name, 'ts_day')

    def test_parse_partition_year(self):
        spec = self.schema_manager._create_partition_spec(self.iceberg_schema, 'year(ts)')
        self.assertIsNotNone(spec)
        self.assertIsInstance(spec.fields[0].transform, YearTransform)
        self.assertEqual(spec.fields[0].name, 'ts_year')

    def test_parse_partition_bucket(self):
        spec = self.schema_manager._create_partition_spec(self.iceberg_schema, 'bucket(16, id)')
        self.assertIsNotNone(spec)
        self.assertIsInstance(spec.fields[0].transform, BucketTransform)
        self.assertEqual(spec.fields[0].transform.num_buckets, 16)
        self.assertEqual(spec.fields[0].name, 'id_bucket_16')

    def test_parse_partition_truncate(self):
        spec = self.schema_manager._create_partition_spec(self.iceberg_schema, 'truncate(4, name)')
        self.assertIsNotNone(spec)
        self.assertIsInstance(spec.fields[0].transform, TruncateTransform)
        self.assertEqual(spec.fields[0].transform.width, 4)
        self.assertEqual(spec.fields[0].name, 'name_trunc_4')

    def test_parse_partition_invalid_syntax(self):
        spec = self.schema_manager._create_partition_spec(self.iceberg_schema, 'unknown_func(ts)')
        self.assertIsNone(spec)

    def test_parse_partition_column_not_found(self):
        spec = self.schema_manager._create_partition_spec(self.iceberg_schema, 'missing_col')
        self.assertIsNone(spec)


if __name__ == '__main__':
    unittest.main()
