"""Register existing Parquet files as Iceberg data files without rewriting them.

This is a metadata-only operation: the files are referenced by the table's metadata and are not copied,
converted, or moved. Their physical location must stay reachable by every reader of the table for as long
as the data is expected to be readable.
"""

from collections.abc import Sequence
from typing import Any, NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import ResolveError
from pyiceberg.schema import Schema, promote
from pyiceberg.types import NestedField

from iceberg_loader.services.logging import logger
from iceberg_loader.utils.types import get_iceberg_type


class _ParquetFile(NamedTuple):
    path: str
    schema: pa.Schema
    num_rows: int


def register_parquet_files(
    file_paths: Sequence[str],
    table_identifier: tuple[str, str],
    catalog: Catalog,
    *,
    snapshot_properties: dict[str, str] | None = None,
    check_duplicate_files: bool = True,
) -> dict[str, Any]:
    """
    Register existing Parquet files in an existing Iceberg table.

    The files are referenced, not rewritten, so they must remain at the given paths and readable by the
    table's readers. Schemas are validated from Parquet footers before registration: unknown columns and
    missing required columns raise ValueError. Column type compatibility, including promotions such as
    int32 -> int64, follows PyIceberg's promotion rules; nested and unmapped columns are left to PyIceberg
    validation. Partition values are inferred from each file's statistics, so PyIceberg rejects a file that
    spans more than one partition.

    Returns a dict with files_added, rows_added, snapshot_id, and table_location.
    """
    paths = list(file_paths)
    if not paths:
        raise ValueError('file_paths cannot be empty')
    if len(paths) != len(set(paths)):
        raise ValueError('file_paths must be unique')

    table = catalog.load_table(table_identifier)
    table_name = '.'.join(table_identifier)

    parquet_files = [_read_parquet_file(path) for path in paths]
    _validate_file_schemas(parquet_files, table.schema(), table_name)

    if not check_duplicate_files:
        logger.warning(
            'check_duplicate_files=False: files already registered in table %s may be added twice and duplicate rows',
            table_name,
        )

    table.add_files(
        paths,
        snapshot_properties=snapshot_properties or {},
        check_duplicate_files=check_duplicate_files,
    )

    snapshot = table.current_snapshot()
    return {
        'files_added': len(paths),
        'rows_added': sum(parquet_file.num_rows for parquet_file in parquet_files),
        'snapshot_id': snapshot.snapshot_id if snapshot else 'none',
        'table_location': table.location(),
    }


def _read_parquet_file(path: str) -> _ParquetFile:
    parquet_file = pq.ParquetFile(path)
    return _ParquetFile(path=path, schema=parquet_file.schema_arrow, num_rows=parquet_file.metadata.num_rows)


def _validate_file_schemas(files: list[_ParquetFile], table_schema: Schema, table_name: str) -> None:
    table_fields = {field.name: field for field in table_schema.fields}
    for parquet_file in files:
        _validate_file_schema(parquet_file, table_fields, table_name)


def _validate_file_schema(
    parquet_file: _ParquetFile,
    table_fields: dict[str, NestedField],
    table_name: str,
) -> None:
    file_fields = {field.name: field for field in parquet_file.schema}

    unknown_columns = sorted(file_fields.keys() - table_fields.keys())
    if unknown_columns:
        raise ValueError(
            f"Parquet file '{parquet_file.path}' has columns not present in table {table_name}: "
            f'{", ".join(unknown_columns)}',
        )

    missing_required_columns = sorted(
        name for name, field in table_fields.items() if field.required and name not in file_fields
    )
    if missing_required_columns:
        raise ValueError(
            f"Parquet file '{parquet_file.path}' is missing required columns of table {table_name}: "
            f'{", ".join(missing_required_columns)}',
        )

    for name, file_field in file_fields.items():
        _validate_column_type(parquet_file.path, name, file_field.type, table_fields[name], table_name)


def _validate_column_type(
    file_path: str,
    column_name: str,
    file_arrow_type: pa.DataType,
    table_field: NestedField,
    table_name: str,
) -> None:
    try:
        file_type = get_iceberg_type(file_arrow_type)
    except ValueError:
        logger.debug(
            "Unmapped Parquet type %s in file '%s'; leaving column %s to PyIceberg validation",
            file_arrow_type,
            file_path,
            column_name,
        )
        return

    if file_type == table_field.field_type:
        return

    try:
        promote(file_type, table_field.field_type)
    except ResolveError as exc:
        raise ValueError(
            f"Parquet file '{file_path}' column '{column_name}' has type {file_type}, "
            f'but table {table_name} expects {table_field.field_type}',
        ) from exc
