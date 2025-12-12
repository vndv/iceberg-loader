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
