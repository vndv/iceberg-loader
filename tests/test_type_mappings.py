import pyarrow as pa
import pyiceberg.types
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.types import (
    BinaryType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
)

from iceberg_loader import LoaderConfig, load_data_to_iceberg
from iceberg_loader.utils.types import get_arrow_type, get_iceberg_type, register_custom_mapping


def test_arrow_to_iceberg_basic() -> None:
    assert get_iceberg_type(pa.string()) == StringType()
    assert get_iceberg_type(pa.large_string()) == StringType()
    assert get_iceberg_type(pa.binary()) == BinaryType()
    assert get_iceberg_type(pa.large_binary()) == BinaryType()
    assert get_iceberg_type(pa.int32()) == IntegerType()
    assert get_iceberg_type(pa.int16()) == IntegerType()
    assert get_iceberg_type(pa.int64()) == LongType()


def test_arrow_to_iceberg_uint64() -> None:
    assert get_iceberg_type(pa.uint64()) == DecimalType(20, 0)


def test_arrow_to_iceberg_timestamp() -> None:
    assert get_iceberg_type(pa.timestamp('us')) == TimestampType()
    assert get_iceberg_type(pa.timestamp('us', tz='UTC')) == TimestamptzType()


def test_iceberg_to_arrow_basic() -> None:
    assert get_arrow_type(StringType()) == pa.string()
    assert get_iceberg_type(pa.int32()) == IntegerType()
    assert get_arrow_type(IntegerType()) == pa.int32()
    assert get_arrow_type(LongType()) == pa.int64()


def test_iceberg_v3_types_to_arrow() -> None:
    v3 = pyiceberg.types
    assert get_arrow_type(v3.TimestampNanoType()) == pa.timestamp('ns')
    assert get_arrow_type(v3.TimestamptzNanoType()) == pa.timestamp('ns', tz='UTC')
    assert get_arrow_type(v3.UnknownType()) == pa.null()


def test_custom_mapping() -> None:
    register_custom_mapping(pa.uint8(), IntegerType())
    assert get_iceberg_type(pa.uint8()) == IntegerType()


def test_unsupported_type() -> None:
    unsupported = pa.duration('s')
    with pytest.raises(ValueError):
        get_iceberg_type(unsupported)


def test_large_string_table_column_is_string(sql_catalog: SqlCatalog) -> None:
    table_id = ('default', 'large_string_table')
    large_strings = pa.table({'s': pa.array(['a', 'b'], type=pa.large_string())})

    load_data_to_iceberg(large_strings, table_id, sql_catalog, LoaderConfig(write_mode='append'))

    result = sql_catalog.load_table(table_id).scan().to_arrow()
    assert result.column('s').to_pylist() == ['a', 'b']
    assert result.schema.field('s').type == pa.string()


def test_large_binary_table_column_is_binary(sql_catalog: SqlCatalog) -> None:
    table_id = ('default', 'large_binary_table')
    large_binary = pa.table({'b': pa.array([b'\x01', b'\x02'], type=pa.large_binary())})

    load_data_to_iceberg(large_binary, table_id, sql_catalog, LoaderConfig(write_mode='append'))

    result = sql_catalog.load_table(table_id).scan().to_arrow()
    assert result.column('b').to_pylist() == [b'\x01', b'\x02']
    assert result.schema.field('b').type == pa.binary()
