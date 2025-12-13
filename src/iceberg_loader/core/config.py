from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

TABLE_PROPERTIES = {
    'write.format.default': 'parquet',
    'format-version': 2,
    'write.parquet.compression-codec': 'zstd',
    'commit.retry.num-retries': 10,
    'commit.retry.min-wait-ms': 100,
    'commit.retry.max-wait-ms': 60000,
}


@dataclass
class LoaderConfig:
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
