"""High-watermark helper for incremental loads into Iceberg tables.

The watermark is the maximum value of a column that is already visible in the destination table.
Typical use is to filter the *source* system with ``column > watermark`` before loading the result,
so that only rows added since the previous run are read. Storing the watermark between runs is the
caller's responsibility (an Airflow Variable/XCom, a database row, an object in S3, and so on);
this module never persists checkpoints.
"""

from __future__ import annotations

from typing import Any, Literal, NamedTuple

import pyarrow.compute as pc
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.types import (
    BinaryType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    NestedField,
    PrimitiveType,
    StringType,
    TimestampNanoType,
    TimestampType,
    TimestamptzNanoType,
    TimestamptzType,
    TimeType,
)

from iceberg_loader.services.logging import logger

WatermarkMode = Literal['auto', 'metadata', 'scan']

_ALLOWED_MODES = ('auto', 'metadata', 'scan')

# Iceberg only collects full, non-truncated bounds for these top-level types. String and binary
# bounds are truncated, UUID and boolean are not numeric/temporal, and nested fields have no
# top-level bound at all.
_METADATA_SAFE_TYPES = (
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    DateType,
    TimeType,
    TimestampType,
    TimestamptzType,
    TimestampNanoType,
    TimestamptzNanoType,
)


class _MetadataSample(NamedTuple):
    """Data-file metadata read once, plus the reason it cannot be trusted (or None)."""

    data_files: list[dict[str, Any]]
    rejection: str | None


def get_high_watermark(
    table_identifier: tuple[str, str],
    column: str,
    catalog: Catalog,
    *,
    mode: WatermarkMode = 'auto',
) -> Any:
    """Return the maximum visible value of a column, for incremental source filtering.

    Use the result as an exclusive lower bound on the *source* query
    (``WHERE column > watermark``) and load only the newer rows. Persisting the watermark
    between runs is the caller's job; this helper only reads the destination table.

    Modes:
        ``scan``: reads the column from the table (``scan(selected_fields=(column,))``) and takes
            its maximum. Works for every primitive type, including ``string``/``binary``, and
            accounts for delete files. Cost is a full column scan.
        ``metadata``: takes the maximum ``upper_bound`` from data-file metadata. Reads only
            manifests and table metadata, so it is much cheaper, but only for top-level numeric or
            temporal columns and only when no delete files exist. Raises ``ValueError`` instead of
            returning a possibly wrong value when those conditions do not hold.
        ``auto`` (default): uses ``metadata`` when its preconditions hold and silently falls back
            to ``scan`` otherwise.

    Returns the value in the column's Python type, or ``None`` when the table has no rows or the
    column is all ``NULL``.

    Nested (struct/list/map) columns raise ``ValueError`` in every mode.

    Correctness caveats:
        * String and binary bounds are truncated, so ``metadata`` mode refuses them.
        * Data-file bounds may describe rows hidden by delete files, so ``metadata`` mode refuses
          tables with delete files. ``scan`` reflects the deletes.
        * ``float``/``double`` watermarks are only correct for finite values: Iceberg excludes NaN
          from float bounds, while Arrow's ``pc.max`` propagates NaN, so the two modes can disagree
          when NaNs are present.
        * A late-arriving row whose timestamp is equal to or below the watermark is not re-read;
          use ``>=`` with a de-duplicating strategy if equal-timestamp late arrivals are possible.

    Raises:
        ValueError: for an unknown, nested, or dotted column, an invalid ``mode``, or a metadata
            request that cannot be trusted.
        NoSuchTableError: when the table does not exist.
    """
    if mode not in _ALLOWED_MODES:
        raise ValueError(f"Invalid watermark mode '{mode}'. Allowed modes: {', '.join(_ALLOWED_MODES)}.")

    table = catalog.load_table(table_identifier)
    field = _resolve_top_level_field(table.schema(), column, table_identifier)

    if mode == 'scan':
        return _max_from_scan(table, column)

    sample = _inspect_metadata(table, field, column)
    if mode == 'metadata':
        _raise_if_metadata_untrusted(sample, table_identifier, column)
        return _max_from_metadata(sample.data_files, column)

    if sample.rejection is not None:
        logger.debug(
            'High watermark for %s.%s falls back to scan: %s',
            '.'.join(table_identifier),
            column,
            sample.rejection,
        )
        return _max_from_scan(table, column)
    return _max_from_metadata(sample.data_files, column)


def _resolve_top_level_field(schema: Schema, column: str, table_identifier: tuple[str, str]) -> NestedField:
    top_level_names = [field.name for field in schema.fields]
    if column not in top_level_names:
        raise ValueError(
            f"Column '{column}' is not a top-level column of table {'.'.join(table_identifier)}. "
            f'Available top-level columns: {", ".join(top_level_names) or "<none>"}. '
            'Nested and dotted column names are not supported.',
        )

    field = next(field for field in schema.fields if field.name == column)
    if not isinstance(field.field_type, PrimitiveType):
        raise ValueError(
            f"Column '{column}' of table {'.'.join(table_identifier)} is not a primitive type "
            f'({type(field.field_type).__name__}); watermarks require a top-level primitive column.',
        )
    return field


def _inspect_metadata(table: Table, field: NestedField, column: str) -> _MetadataSample:
    """Read the data files once and decide whether their bounds can be trusted."""
    data_files = table.inspect.data_files().to_pylist()
    return _MetadataSample(
        data_files=data_files,
        rejection=_metadata_precondition_failure(table, field, column, data_files),
    )


def _metadata_precondition_failure(
    table: Table,
    field: NestedField,
    column: str,
    data_files: list[dict[str, Any]],
) -> str | None:
    """Return why ``metadata`` mode cannot be trusted, or ``None`` when it can.

    An empty data-file list is not a failure: there are simply no rows, so the watermark is None.
    """
    if not data_files:
        return None

    if not isinstance(field.field_type, _METADATA_SAFE_TYPES):
        reason = (
            'string and binary bounds may be truncated'
            if isinstance(field.field_type, (StringType, BinaryType))
            else 'not a supported watermark type'
        )
        return (
            f"column '{column}' has type {type(field.field_type).__name__}; "
            f'metadata bounds are only trusted for top-level numeric and temporal columns ({reason})'
        )

    delete_file_count = table.inspect.delete_files().num_rows
    if delete_file_count > 0:
        return (
            f'the table has {delete_file_count} delete file(s), so data-file bounds may include rows hidden by deletes'
        )

    for data_file in data_files:
        metrics = _readable_metrics(data_file, column)
        if metrics is None:
            return f"data file {data_file['file_path']} has no readable metrics for column '{column}'"
        if metrics['upper_bound'] is not None:
            continue
        if _is_all_null(metrics):
            continue
        return f"data file {data_file['file_path']} has no upper bound for column '{column}'"

    return None


def _readable_metrics(data_file: dict[str, Any], column: str) -> dict[str, Any] | None:
    readable_metrics = data_file.get('readable_metrics') or {}
    metrics = readable_metrics.get(column)
    return metrics if isinstance(metrics, dict) else None


def _is_all_null(metrics: dict[str, Any]) -> bool:
    value_count = metrics.get('value_count')
    null_value_count = metrics.get('null_value_count')
    return value_count is not None and value_count == null_value_count


def _raise_if_metadata_untrusted(
    sample: _MetadataSample,
    table_identifier: tuple[str, str],
    column: str,
) -> None:
    if sample.rejection is None:
        return
    raise ValueError(
        f"Cannot use metadata for a high watermark of column '{column}' in table "
        f'{".".join(table_identifier)}: {sample.rejection}. '
        "Use mode='scan' to read the visible rows.",
    )


def _max_from_metadata(data_files: list[dict[str, Any]], column: str) -> Any:
    """Return the largest data-file upper bound, ignoring files where the column is all NULL."""
    bounds = [upper_bound for data_file in data_files if (upper_bound := _upper_bound(data_file, column)) is not None]
    return max(bounds) if bounds else None


def _upper_bound(data_file: dict[str, Any], column: str) -> Any:
    metrics = _readable_metrics(data_file, column)
    return None if metrics is None else metrics['upper_bound']


def _max_from_scan(table: Table, column: str) -> Any:
    """Scan the visible column and return its maximum, or None for no/all-NULL rows."""
    values = table.scan(selected_fields=(column,)).to_arrow().column(column)
    if len(values) == 0 or values.null_count == len(values):
        return None
    # PyArrow's type stubs do not declare the dynamically generated compute functions.
    return pc.max(values).as_py()  # ty: ignore[unresolved-attribute]
