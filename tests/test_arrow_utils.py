from collections.abc import Callable

import pyarrow as pa
import pytest

from iceberg_loader.services.logging import configure_logging
from iceberg_loader.utils.arrow import (
    convert_column_type,
    convert_table_types,
    create_arrow_table_from_data,
    create_record_batches_from_dicts,
)


@pytest.fixture(scope='module', autouse=True)
def _setup_logging() -> None:
    configure_logging()


def test_create_arrow_table_basic() -> None:
    data = [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
    table = create_arrow_table_from_data(data)

    assert table.num_rows == 2
    assert set(table.schema.names) == {'id', 'name'}


def test_create_arrow_table_orders_columns_by_name() -> None:
    names = [f'col_{i:02d}' for i in range(20)]
    data = [dict.fromkeys(reversed(names[:10]), 1), dict.fromkeys(names[5:], 1)]

    table = create_arrow_table_from_data(data)

    assert table.column_names == names


def test_create_record_batches_same_schema_regardless_of_key_order() -> None:
    rows = [{'a': 1, 'b': 2}, {'a': 3, 'b': 4}, {'b': 5, 'a': 6}, {'b': 7, 'a': 8}]

    batches = list(create_record_batches_from_dicts(iter(rows), batch_size=2))

    assert batches[0].schema.equals(batches[1].schema)


def test_create_arrow_table_empty() -> None:
    data: list[dict[str, object]] = []
    table = create_arrow_table_from_data(data)

    assert table.num_rows == 0
    assert len(table.schema) == 0


def test_create_arrow_table_serializes_complex() -> None:
    data = [
        {'id': 1, 'complex_field': {'a': 1, 'b': 'x'}},
        {'id': 2, 'complex_field': [1, 2, 3]},
    ]

    table = create_arrow_table_from_data(data)

    assert table.schema.field('complex_field').type == pa.string()
    assert table.to_pydict()['complex_field'] == ['{"a":1,"b":"x"}', '[1,2,3]']


def test_create_record_batches_from_dicts() -> None:
    data = [{'id': i, 'value': f'v{i}'} for i in range(25)]
    batches = list(create_record_batches_from_dicts(iter(data), batch_size=10))

    assert len(batches) == 3
    assert batches[0].num_rows == 10
    assert batches[1].num_rows == 10
    assert batches[2].num_rows == 5


def test_create_record_batches_from_dicts_empty() -> None:
    data: list[dict[str, object]] = []
    batches = list(create_record_batches_from_dicts(iter(data), batch_size=10))

    assert len(batches) == 0


def test_convert_column_type_warns_and_nulls(caplog: pytest.LogCaptureFixture) -> None:
    column = pa.array(['a', 'b'])
    with caplog.at_level('WARNING', logger='iceberg_loader'):
        converted = convert_column_type(column, pa.int64(), 'bad')
    assert converted.to_pylist() == [None, None]


def test_convert_column_type_warns_on_timestamp_truncation(loader_warnings: Callable[[], list[str]]) -> None:
    column = pa.array([1_700_000_000_123_456_789], type=pa.timestamp('ns'))
    converted = convert_column_type(column, pa.timestamp('us'), 'ts')
    assert converted.cast(pa.int64()).to_pylist() == [1_700_000_000_123_456]
    assert loader_warnings() == [
        'Timestamp precision lost for column ts (timestamp[ns] -> timestamp[us]). Values were truncated.',
    ]


def test_convert_column_type_exact_timestamp_does_not_warn(loader_warnings: Callable[[], list[str]]) -> None:
    column = pa.array([1_700_000_000_123_456_000], type=pa.timestamp('ns'))
    convert_column_type(column, pa.timestamp('us'), 'ts')
    assert loader_warnings() == []


def test_convert_column_type_widening_overflow_is_not_called_truncation(
    loader_warnings: Callable[[], list[str]],
) -> None:
    year_3000_us = 32_503_680_000_000_000
    convert_column_type(pa.array([year_3000_us], type=pa.timestamp('us')), pa.timestamp('ns'), 'ts')
    assert not any('Timestamp precision lost' in m for m in loader_warnings())


def test_convert_table_types_missing_column() -> None:
    table = pa.Table.from_pydict({'a': [1, 2]})
    target = pa.schema([pa.field('a', pa.int64()), pa.field('b', pa.string())])
    converted = convert_table_types(table, target)
    assert converted.column('b').to_pylist() == [None, None]


def test_convert_table_types_cast_error_branch(caplog: pytest.LogCaptureFixture) -> None:
    table = pa.Table.from_pydict({'a': ['x', 'y']})
    target = pa.schema([pa.field('a', pa.int64())])
    with caplog.at_level('WARNING', logger='iceberg_loader'):
        converted = convert_table_types(table, target)
    assert converted.column('a').to_pylist() == [None, None]
