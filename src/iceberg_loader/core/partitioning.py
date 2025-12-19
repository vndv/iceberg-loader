from __future__ import annotations

import re

from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Transform,
    TruncateTransform,
    VoidTransform,
    YearTransform,
)
from pyiceberg.types import DateType, TimestampType, TimestamptzType

_PARTITION_PATTERN = re.compile(r'(\w+)\((.+)\)')


def parse_partition_transform(partition_str: str) -> tuple[str, str, int | None]:
    """
    Parses strings like:
    - "ts" -> ("identity", "ts", None)
    - "month(ts)" -> ("month", "ts", None)
    - "bucket(16, id)" -> ("bucket", "id", 16)
    - "truncate(4, name)" -> ("truncate", "name", 4)
    """
    cleaned = partition_str.strip()
    if not cleaned:
        raise ValueError('partition_col cannot be empty')

    match = _PARTITION_PATTERN.match(cleaned)
    if not match:
        # Looks like a function call but failed to parse -> reject early
        if '(' in cleaned or ')' in cleaned:
            raise ValueError(f'Invalid partition expression: {partition_str}')
        return 'identity', cleaned, None

    func_name = match.group(1).lower()
    args_str = match.group(2)
    args = [a.strip() for a in args_str.split(',')]

    if func_name in ('year', 'month', 'day', 'hour', 'void'):
        if len(args) != 1:
            raise ValueError(f"Transform '{func_name}' expects 1 argument, got {len(args)}")
        return func_name, args[0], None

    if func_name in ('bucket', 'truncate'):
        return _parse_param_transform(func_name, args)

    raise ValueError(f'Unknown partition transform: {func_name}')


def _parse_param_transform(func_name: str, args: list[str]) -> tuple[str, str, int]:
    if len(args) != 2:
        raise ValueError(f"Transform '{func_name}' expects 2 arguments (param, col), got {len(args)}")
    try:
        param = int(args[0])
    except ValueError as e:
        raise ValueError(f"First argument for '{func_name}' must be an integer, got '{args[0]}'") from e
    return func_name, args[1], param


def get_transform_impl(transform_name: str, param: int | None) -> Transform:
    if transform_name == 'identity':
        return IdentityTransform()
    if transform_name == 'year':
        return YearTransform()
    if transform_name == 'month':
        return MonthTransform()
    if transform_name == 'day':
        return DayTransform()
    if transform_name == 'hour':
        return HourTransform()
    if transform_name == 'bucket':
        if param is None:
            raise ValueError('Bucket transform requires a width parameter')
        return BucketTransform(num_buckets=param)
    if transform_name == 'truncate':
        if param is None:
            raise ValueError('Truncate transform requires a width parameter')
        return TruncateTransform(width=param)
    if transform_name == 'void':
        return VoidTransform()
    raise ValueError(f'Unsupported transform implementation: {transform_name}')


def is_timestamp_identity(transform: str, field_type: object) -> bool:
    return transform == 'identity' and isinstance(field_type, TimestampType | TimestamptzType)


def transform_supports_type(transform: str, field_type: object) -> bool:
    if transform in ('year', 'month', 'day', 'hour'):
        return isinstance(field_type, DateType | TimestampType | TimestamptzType)
    return True
