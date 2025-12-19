from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from iceberg_loader.core.partitioning import parse_partition_transform

TABLE_PROPERTIES = {
    'write.format.default': 'parquet',
    'format-version': 2,
    'write.parquet.compression-codec': 'zstd',
    'commit.retry.num-retries': 10,
    'commit.retry.min-wait-ms': 100,
    'commit.retry.max-wait-ms': 60000,
}


class LoaderConfig(BaseModel):
    """Defaults for IcebergLoader operations."""

    write_mode: Literal['overwrite', 'append', 'upsert'] = 'overwrite'
    partition_col: str | None = None
    replace_filter: str | None = None
    schema_evolution: bool = False
    table_properties: dict[str, Any] | None = None
    commit_interval: int = 0
    join_cols: list[str] | None = None
    load_timestamp: datetime | None = None
    load_ts_col: str = '_load_dttm'

    model_config = ConfigDict(extra='forbid', frozen=True, str_strip_whitespace=True)

    @field_validator('commit_interval')
    @classmethod
    def validate_commit_interval(cls, value: int) -> int:
        if value < 0:
            raise ValueError('commit_interval must be >= 0')
        return value

    @field_validator('partition_col')
    @classmethod
    def validate_partition_col(cls, value: str | None) -> str | None:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            raise ValueError('partition_col cannot be empty')
        parse_partition_transform(cleaned)
        return cleaned

    @field_validator('load_ts_col')
    @classmethod
    def validate_load_ts_col(cls, value: str) -> str:
        if not re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', value):
            raise ValueError('load_ts_col must start with a letter/underscore and contain only alphanumerics/_')
        return value

    @field_validator('join_cols')
    @classmethod
    def validate_join_cols(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return value
        cleaned = [v.strip() for v in value if v and v.strip()]
        if not cleaned:
            raise ValueError('join_cols cannot be empty if provided')
        return cleaned

    @field_validator('table_properties')
    @classmethod
    def validate_table_properties(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        if value is None:
            return value
        if not all(isinstance(k, str) for k in value):
            raise ValueError('table_properties keys must be strings')
        return value

    @model_validator(mode='after')
    def validate_combinations(self) -> LoaderConfig:
        if self.write_mode == 'upsert' and self.replace_filter:
            raise ValueError("replace_filter cannot be used with write_mode='upsert'")

        if self.load_timestamp and self.partition_col:
            transform, source_col, _ = parse_partition_transform(self.partition_col)
            if transform == 'identity' and source_col == self.load_ts_col:
                raise ValueError(
                    'partition_col uses identity on the load timestamp column; use day(...) or hour(...) instead.',
                )

        return self


def ensure_loader_config(config: Any) -> LoaderConfig:
    """Validate and normalize LoaderConfig instances."""
    if config is None:
        return LoaderConfig()
    if isinstance(config, LoaderConfig):
        return config

    raise TypeError('config must be a LoaderConfig instance')
