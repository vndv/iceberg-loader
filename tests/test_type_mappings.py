import pyarrow as pa
import pytest
from pyiceberg.types import DecimalType, IntegerType, LongType, StringType, TimestampType, TimestamptzType

from iceberg_loader.utils.types import get_arrow_type, get_iceberg_type, register_custom_mapping


def test_arrow_to_iceberg_basic() -> None:
    assert get_iceberg_type(pa.string()) == StringType()
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


def test_custom_mapping() -> None:
    register_custom_mapping(pa.uint8(), IntegerType())
    assert get_iceberg_type(pa.uint8()) == IntegerType()


def test_unsupported_type() -> None:
    unsupported = pa.duration('s')
    with pytest.raises(ValueError):
        get_iceberg_type(unsupported)
