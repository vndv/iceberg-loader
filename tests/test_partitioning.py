from unittest.mock import MagicMock

import pyarrow as pa
import pytest
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema as IcebergSchema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    IdentityTransform,
    MonthTransform,
    TruncateTransform,
    YearTransform,
)

from iceberg_loader.core.schema import SchemaManager


@pytest.fixture()
def schema_manager() -> SchemaManager:
    return SchemaManager(MagicMock(), {})


@pytest.fixture()
def iceberg_schema(schema_manager: SchemaManager) -> IcebergSchema:
    arrow_schema = pa.schema(
        [
            pa.field('id', pa.int64()),
            pa.field('ts', pa.timestamp('us')),
            pa.field('name', pa.string()),
        ],
    )
    return schema_manager._arrow_to_iceberg(arrow_schema)


def test_parse_partition_identity(schema_manager: SchemaManager, iceberg_schema: IcebergSchema) -> None:
    spec = schema_manager._create_partition_spec(iceberg_schema, 'ts')
    assert spec is not None
    assert isinstance(spec, PartitionSpec)
    assert len(spec.fields) == 1
    assert isinstance(spec.fields[0].transform, IdentityTransform)
    assert spec.fields[0].name == 'ts'


def test_parse_partition_month(schema_manager: SchemaManager, iceberg_schema: IcebergSchema) -> None:
    spec = schema_manager._create_partition_spec(iceberg_schema, 'month(ts)')
    assert spec is not None
    assert isinstance(spec.fields[0].transform, MonthTransform)
    assert spec.fields[0].name == 'ts_month'


def test_parse_partition_day(schema_manager: SchemaManager, iceberg_schema: IcebergSchema) -> None:
    spec = schema_manager._create_partition_spec(iceberg_schema, 'day(ts)')
    assert spec is not None
    assert isinstance(spec.fields[0].transform, DayTransform)
    assert spec.fields[0].name == 'ts_day'


def test_parse_partition_year(schema_manager: SchemaManager, iceberg_schema: IcebergSchema) -> None:
    spec = schema_manager._create_partition_spec(iceberg_schema, 'year(ts)')
    assert spec is not None
    assert isinstance(spec.fields[0].transform, YearTransform)
    assert spec.fields[0].name == 'ts_year'


def test_parse_partition_bucket(schema_manager: SchemaManager, iceberg_schema: IcebergSchema) -> None:
    spec = schema_manager._create_partition_spec(iceberg_schema, 'bucket(16, id)')
    assert spec is not None
    assert isinstance(spec.fields[0].transform, BucketTransform)
    assert spec.fields[0].transform.num_buckets == 16
    assert spec.fields[0].name == 'id_bucket_16'


def test_parse_partition_truncate(schema_manager: SchemaManager, iceberg_schema: IcebergSchema) -> None:
    spec = schema_manager._create_partition_spec(iceberg_schema, 'truncate(4, name)')
    assert spec is not None
    assert isinstance(spec.fields[0].transform, TruncateTransform)
    assert spec.fields[0].transform.width == 4
    assert spec.fields[0].name == 'name_trunc_4'


def test_parse_partition_invalid_syntax(schema_manager: SchemaManager, iceberg_schema: IcebergSchema) -> None:
    spec = schema_manager._create_partition_spec(iceberg_schema, 'unknown_func(ts)')
    assert spec is None


def test_parse_partition_column_not_found(schema_manager: SchemaManager, iceberg_schema: IcebergSchema) -> None:
    spec = schema_manager._create_partition_spec(iceberg_schema, 'missing_col')
    assert spec is None
