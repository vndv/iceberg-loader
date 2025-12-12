from collections.abc import Iterator
from typing import Any, BinaryIO, Literal

import pyarrow as pa
from pyiceberg.catalog import Catalog

from iceberg_loader.core.config import LoaderConfig
from iceberg_loader.core.loader import IcebergLoader as CoreIcebergLoader

IcebergLoader = CoreIcebergLoader


def load_data_to_iceberg(
    table_data: pa.Table,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    write_mode: Literal['overwrite', 'append', 'upsert'] | None = None,
    partition_col: str | None = None,
    replace_filter: str | None = None,
    schema_evolution: bool | None = None,
    table_properties: dict[str, Any] | None = None,
    join_cols: list[str] | None = None,
    config: LoaderConfig | None = None,
) -> dict[str, Any]:
    loader = IcebergLoader(catalog, table_properties, default_config=config)
    return loader.load_data(
        table_data,
        table_identifier,
        write_mode,
        partition_col,
        replace_filter,
        schema_evolution,
        table_properties,
        join_cols,
        config=config,
    )


def load_batches_to_iceberg(
    batch_iterator: pa.RecordBatchReader | Iterator[pa.RecordBatch],
    table_identifier: tuple[str, str],
    catalog: Catalog,
    write_mode: Literal['overwrite', 'append', 'upsert'] | None = None,
    partition_col: str | None = None,
    replace_filter: str | None = None,
    schema_evolution: bool | None = None,
    table_properties: dict[str, Any] | None = None,
    commit_interval: int | None = None,
    join_cols: list[str] | None = None,
    config: LoaderConfig | None = None,
) -> dict[str, Any]:
    loader = IcebergLoader(catalog, table_properties, default_config=config)
    return loader.load_data_batches(
        batch_iterator,
        table_identifier,
        write_mode,
        partition_col,
        replace_filter,
        schema_evolution,
        commit_interval,
        join_cols,
        table_properties,
        config,
    )


def load_ipc_stream_to_iceberg(
    stream_source: str | BinaryIO | pa.NativeFile,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    write_mode: Literal['overwrite', 'append', 'upsert'] | None = None,
    partition_col: str | None = None,
    replace_filter: str | None = None,
    schema_evolution: bool | None = None,
    table_properties: dict[str, Any] | None = None,
    commit_interval: int | None = None,
    join_cols: list[str] | None = None,
    config: LoaderConfig | None = None,
) -> dict[str, Any]:
    loader = IcebergLoader(catalog, table_properties, default_config=config)
    return loader.load_ipc_stream(
        stream_source,
        table_identifier,
        write_mode,
        partition_col,
        replace_filter,
        schema_evolution,
        commit_interval,
        join_cols,
        table_properties,
        config,
    )


__all__ = [
    'IcebergLoader',
    'load_batches_to_iceberg',
    'load_data_to_iceberg',
    'load_ipc_stream_to_iceberg',
]
