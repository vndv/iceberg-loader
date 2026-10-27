"""Register existing Parquet files as Iceberg data files without rewriting them.

This is a metadata-only operation: the files are referenced by the table's metadata and are not copied,
converted, or moved. Their physical location must stay reachable by every reader of the table for as long
as the data is expected to be readable.
"""

import posixpath
from collections.abc import Sequence
from typing import Any, NamedTuple
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import ResolveError
from pyiceberg.io import FileIO
from pyiceberg.schema import Schema, promote
from pyiceberg.table import Table
from pyiceberg.types import NestedField, TimestampNanoType, TimestamptzNanoType

from iceberg_loader.services.logging import logger
from iceberg_loader.utils.types import get_iceberg_type

_PARQUET_FIELD_ID_KEY = b'PARQUET:field_id'
_NANO_TIMESTAMP_TYPES = (TimestampNanoType, TimestamptzNanoType)


class _ParquetFile(NamedTuple):
    path: str
    schema: pa.Schema
    metadata: pq.FileMetaData


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
    table's readers. Every footer is read through the table's FileIO and validated before anything is
    committed, so one bad file leaves the table unchanged. Most schema checks repeat PyIceberg's own and
    exist to name the file and column in the error; on top of that this function rejects files that
    PyIceberg would accept but read back wrong:

    * files with embedded Parquet field IDs (PyIceberg maps columns by ID, not name);
    * files without min/max statistics for a partition source column (the partition value would be NULL
      and filtered reads would skip the rows);
    * relative paths, and paths that resolve to a file already in the call or, with
      ``check_duplicate_files=True``, already in the table (``/a.parquet`` and ``file:///a.parquet`` are
      the same file).

    Partition values are inferred from each file's statistics, so PyIceberg rejects a file that spans more
    than one partition.

    Returns a dict with files_added, rows_added, snapshot_id, and table_location.
    """
    if isinstance(file_paths, str):
        raise TypeError('file_paths must be a sequence of paths, not a single string')
    paths = list(file_paths)
    if not paths:
        raise ValueError('file_paths cannot be empty')
    _check_unique_paths(paths)

    table = catalog.load_table(table_identifier)
    table_name = '.'.join(table_identifier)

    parquet_files = [_read_parquet_file(table.io, path) for path in paths]
    _validate_file_schemas(parquet_files, table.schema(), table_name)
    _validate_partition_statistics(parquet_files, table, table_name)

    if check_duplicate_files:
        _check_not_registered(paths, table, table_name)
    else:
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
        'rows_added': sum(parquet_file.metadata.num_rows for parquet_file in parquet_files),
        'snapshot_id': snapshot.snapshot_id if snapshot else 'none',
        'table_location': table.location(),
    }


def _canonical_path(path: str) -> str:
    """Return a comparison key for a path: local paths and file:// URIs of the same file match."""
    parsed = urlparse(path)
    if parsed.scheme in ('', 'file'):
        local_path = parsed.path if parsed.scheme else path
        if not posixpath.isabs(local_path):
            raise ValueError(
                f"Parquet file path '{path}' is relative; the table stores paths as given, so other readers "
                'could not find it. Pass an absolute path or a URI.',
            )
        return posixpath.normpath(local_path)
    return f'{parsed.scheme}://{parsed.netloc}{posixpath.normpath(parsed.path)}'


def _check_unique_paths(paths: list[str]) -> None:
    seen: dict[str, str] = {}
    for path in paths:
        key = _canonical_path(path)
        if key in seen:
            raise ValueError(f"file_paths must be unique: '{seen[key]}' and '{path}' refer to the same file")
        seen[key] = path


def _check_not_registered(paths: list[str], table: Table, table_name: str) -> None:
    snapshot = table.current_snapshot()
    if snapshot is None:
        return
    registered = {
        _canonical_path(entry.data_file.file_path)
        for manifest in snapshot.manifests(table.io)
        for entry in manifest.fetch_manifest_entry(table.io, discard_deleted=True)
    }
    duplicates = [path for path in paths if _canonical_path(path) in registered]
    if duplicates:
        raise ValueError(f'Parquet files already referenced by table {table_name}: {", ".join(duplicates)}')


def _read_parquet_file(io: FileIO, path: str) -> _ParquetFile:
    with io.new_input(path).open() as stream:
        metadata = pq.read_metadata(stream)
    return _ParquetFile(path=path, schema=metadata.schema.to_arrow_schema(), metadata=metadata)


def _has_field_id(field: pa.Field) -> bool:
    if field.metadata and _PARQUET_FIELD_ID_KEY in field.metadata:
        return True
    field_type = field.type
    if pa.types.is_struct(field_type):
        return any(_has_field_id(field_type.field(i)) for i in range(field_type.num_fields))
    if pa.types.is_list(field_type) or pa.types.is_large_list(field_type) or pa.types.is_fixed_size_list(field_type):
        return _has_field_id(field_type.value_field)
    if pa.types.is_map(field_type):
        return _has_field_id(field_type.key_field) or _has_field_id(field_type.item_field)
    return False


def _validate_partition_statistics(files: list[_ParquetFile], table: Table, table_name: str) -> None:
    """Reject files whose partition value PyIceberg would infer as NULL despite non-NULL rows."""
    schema = table.schema()
    source_columns = {schema.find_column_name(field.source_id) for field in table.spec().fields}
    for parquet_file in files:
        for row_group_index in range(parquet_file.metadata.num_row_groups):
            row_group = parquet_file.metadata.row_group(row_group_index)
            for column_index in range(row_group.num_columns):
                column = row_group.column(column_index)
                if column.path_in_schema not in source_columns:
                    continue
                statistics = column.statistics
                if statistics is not None and statistics.has_min_max:
                    continue
                if statistics is not None and statistics.has_null_count and statistics.null_count == row_group.num_rows:
                    continue
                raise ValueError(
                    f"Parquet file '{parquet_file.path}' has no min/max statistics for partition source column "
                    f"'{column.path_in_schema}' of table {table_name}, so its partition value cannot be inferred. "
                    'Rewrite the file with statistics enabled.',
                )


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

    id_columns = sorted(name for name, field in file_fields.items() if _has_field_id(field))
    if id_columns:
        raise ValueError(
            f"Parquet file '{parquet_file.path}' has embedded field IDs (columns: {', '.join(id_columns)}). "
            'PyIceberg maps such files by ID instead of by name, so the columns could be read swapped or '
            'as NULL. Rewrite the file without field IDs, or load it with load_data_to_iceberg.',
        )

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

    optional_required_columns = sorted(
        name for name, field in file_fields.items() if field.nullable and table_fields[name].required
    )
    if optional_required_columns:
        raise ValueError(
            f"Parquet file '{parquet_file.path}' stores required columns of table {table_name} as optional: "
            f'{", ".join(optional_required_columns)}',
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
    if (
        pa.types.is_timestamp(file_arrow_type)
        and file_arrow_type.unit == 'ns'
        and not isinstance(table_field.field_type, _NANO_TIMESTAMP_TYPES)
    ):
        raise ValueError(
            f"Parquet file '{file_path}' column '{column_name}' has nanosecond timestamps, "
            f'but table {table_name} expects {table_field.field_type}; PyIceberg cannot register them. '
            'Rewrite the column with microsecond precision.',
        )

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
