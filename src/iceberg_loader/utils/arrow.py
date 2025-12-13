import contextlib
import json
from collections.abc import Iterator
from functools import partial
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from iceberg_loader.services.logging import logger

_json_dumps = partial(json.dumps, ensure_ascii=False, separators=(',', ':'))


def _get_memory_pool() -> pa.MemoryPool:
    return pa.system_memory_pool()


def create_arrow_table_from_data(data: list[dict[str, Any]]) -> pa.Table:
    """Create table from dicts, serializing complex values to JSON strings."""
    if not data:
        return pa.Table.from_arrays([], schema=pa.schema([]))
    return _create_table_native(data)


def _create_table_native(data: list[dict[str, Any]]) -> pa.Table:
    if not data:
        return pa.Table.from_arrays([], schema=pa.schema([]))

    all_keys = set().union(*(d.keys() for d in data))
    arrays = []
    fields = []
    pool = _get_memory_pool()

    for key in all_keys:
        column_values: list[str | None] = []
        for item in data:
            value = item.get(key)
            if isinstance(value, dict | list):
                column_values.append(_json_dumps(value))
            else:
                column_values.append(str(value) if value is not None else None)

        try:
            array = pa.array(column_values, memory_pool=pool)
            field = pa.field(key, array.type, nullable=True)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            str_values = [str(v) if v is not None else None for v in column_values]
            array = pa.array(str_values, type=pa.string(), memory_pool=pool)
            field = pa.field(key, pa.string(), nullable=True)

        if pa.types.is_null(array.type):
            array = pa.nulls(len(column_values), type=pa.string(), memory_pool=pool)
            field = pa.field(key, pa.string(), nullable=True)

        arrays.append(array)
        fields.append(field)

    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def convert_column_type(column: pa.Array, target_type: pa.DataType, column_name: str | None = None) -> pa.Array:
    """Casts a single column to the target type, handling errors gracefully."""
    if column.type.equals(target_type):
        return column

    try:
        return pc.cast(column, target_type, safe=True)
    except (ValueError, TypeError, pa.ArrowInvalid):
        pass

    try:
        return pc.cast(column, target_type, safe=False)
    except (ValueError, TypeError, pa.ArrowInvalid):
        pass

    if (pa.types.is_string(column.type) or pa.types.is_large_string(column.type)) and (
        pa.types.is_timestamp(target_type) or pa.types.is_date(target_type)
    ):
        with contextlib.suppress(Exception):
            pass

    logger.warning(
        'Cast failed for column %s (%s -> %s). Filling with NULLs.',
        column_name or '<unknown>',
        column.type,
        target_type,
    )
    return pa.nulls(len(column), type=target_type, memory_pool=_get_memory_pool())


def convert_table_types(table: pa.Table, target_schema: pa.Schema) -> pa.Table:
    if table.schema.equals(target_schema):
        return table
    return _convert_table_types_internal(table, target_schema)


def _convert_table_types_internal(table: pa.Table, target_schema: pa.Schema) -> pa.Table:
    source_fields = {field.name: field for field in table.schema}
    new_arrays, new_fields = [], []

    for field in target_schema:
        if field.name in source_fields:
            source_field = source_fields[field.name]
            column = table[field.name]

            if source_field.type.equals(field.type):
                new_arrays.append(column)
                new_fields.append(field)
            else:
                new_array = convert_column_type(column, field.type, field.name)
                new_arrays.append(new_array)
                new_fields.append(
                    pa.field(field.name, new_array.type, nullable=field.nullable, metadata=field.metadata),
                )
        else:
            null_array = pa.nulls(len(table), type=field.type, memory_pool=_get_memory_pool())
            new_arrays.append(null_array)
            new_fields.append(field)

    return pa.Table.from_arrays(new_arrays, schema=pa.schema(new_fields))


def create_record_batches_from_dicts(
    data_iterator: Iterator[dict[str, Any]],
    batch_size: int = 10000,
) -> Iterator[pa.RecordBatch]:
    """Convert iterator of dicts to PyArrow RecordBatches with specified batch size."""
    batch = []
    for item in data_iterator:
        batch.append(item)
        if len(batch) >= batch_size:
            table = create_arrow_table_from_data(batch)
            batches = table.to_batches(max_chunksize=batch_size)
            if batches:
                yield batches[0]
            batch = []

    if batch:
        table = create_arrow_table_from_data(batch)
        batches = table.to_batches(max_chunksize=len(batch))
        if batches:
            yield batches[0]
