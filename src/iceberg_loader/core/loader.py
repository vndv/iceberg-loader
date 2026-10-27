import time
from collections.abc import Iterable, Iterator
from typing import Any, BinaryIO

import pyarrow as pa
from pyiceberg.catalog import Catalog

from iceberg_loader.core.config import TABLE_PROPERTIES, LoaderConfig, ensure_loader_config
from iceberg_loader.core.conform import BatchConformer
from iceberg_loader.core.schema import SchemaManager
from iceberg_loader.core.strategies import get_write_strategy
from iceberg_loader.services.logging import metrics


class IcebergLoader:
    """
    Facade for loading data into Iceberg tables.
    Buffers batches, conforms each buffer to the table via BatchConformer, and writes it via a WriteStrategy.
    """

    def __init__(
        self,
        catalog: Catalog,
        table_properties: dict[str, Any] | None = None,
        default_config: LoaderConfig | None = None,
    ):
        self.catalog = catalog
        self.table_properties = TABLE_PROPERTIES.copy()
        if table_properties:
            self.table_properties.update(table_properties)
        self._user_table_properties = dict(table_properties or {})

        self.schema_manager = SchemaManager(self.catalog, self.table_properties)
        self.default_config = ensure_loader_config(default_config)

    def _resolve_config(self, config: LoaderConfig | None) -> LoaderConfig:
        if config is None:
            return self.default_config
        return ensure_loader_config(config)

    def load_data(
        self,
        table_data: pa.Table,
        table_identifier: tuple[str, str],
        config: LoaderConfig | None = None,
    ) -> dict[str, Any]:
        """
        Load PyArrow Table into Iceberg table.
        Delegates to load_data_batches for consistency.
        """
        batches = table_data.to_batches()
        return self.load_data_batches(
            batch_iterator=iter(batches),
            table_identifier=table_identifier,
            config=config,
        )

    def load_ipc_stream(
        self,
        stream_source: str | BinaryIO | pa.NativeFile,
        table_identifier: tuple[str, str],
        config: LoaderConfig | None = None,
    ) -> dict[str, Any]:
        """Loads data from an Apache Arrow IPC stream source."""
        with pa.ipc.open_stream(stream_source) as reader:
            return self.load_data_batches(
                batch_iterator=reader,
                table_identifier=table_identifier,
                config=config,
            )

    def load_data_batches(
        self,
        batch_iterator: Iterator[pa.RecordBatch] | pa.RecordBatchReader,
        table_identifier: tuple[str, str],
        config: LoaderConfig | None = None,
    ) -> dict[str, Any]:
        """
        Main orchestration method.
        Buffers batches up to commit_interval, conforms each buffer to the table, and delegates writing.
        """
        effective_config = self._resolve_config(config)
        effective_table_properties = self.table_properties.copy()
        user_table_properties = self._user_table_properties.copy()
        if effective_config.table_properties:
            effective_table_properties.update(effective_config.table_properties)
            user_table_properties.update(effective_config.table_properties)

        total_start = time.perf_counter()

        strategy = get_write_strategy(
            effective_config.write_mode,
            effective_config.replace_filter,
            effective_config.join_cols,
        )
        conformer = BatchConformer(
            self.schema_manager,
            table_identifier,
            effective_config,
            effective_table_properties,
            requested_format_version=user_table_properties.get('format-version'),
        )

        batches_processed = 0
        total_rows = 0
        conform_ms = 0.0
        write_ms = 0.0
        is_first_write = True
        for buffer in _buffered(batch_iterator, max(1, effective_config.commit_interval)):
            conform_start = time.perf_counter()
            data = conformer.conform(buffer)
            conform_ms += (time.perf_counter() - conform_start) * 1000

            write_start = time.perf_counter()
            strategy.write(conformer.table, data, is_first_write)
            write_ms += (time.perf_counter() - write_start) * 1000

            is_first_write = False
            batches_processed += len(buffer)
            total_rows += len(data)

        # conformer.table stays None when the iterator yielded no batches.
        snapshot_id = 'none'
        table_loc = 'none'
        if conformer.table is not None:
            table_loc = conformer.table.location()
            current_snap = conformer.table.current_snapshot()
            if current_snap:
                snapshot_id = current_snap.snapshot_id

        total_ms = (time.perf_counter() - total_start) * 1000
        conform_ms = round(conform_ms, 3)
        write_ms = round(write_ms, 3)
        total_ms = round(total_ms, 3)
        casts_applied = conformer.cast_stats.applied
        casts_lossy = conformer.cast_stats.lossy
        casts_failed = conformer.cast_stats.failed

        metrics(
            'load_completed',
            {
                'rows_loaded': total_rows,
                'batches_processed': batches_processed,
                'write_mode': effective_config.write_mode,
                'conform_ms': conform_ms,
                'write_ms': write_ms,
                'total_ms': total_ms,
                'casts_applied': casts_applied,
                'casts_lossy': casts_lossy,
                'casts_failed': casts_failed,
            },
            stacklevel=2,
        )

        return {
            'rows_loaded': total_rows,
            'write_mode': effective_config.write_mode,
            'partition_col': effective_config.partition_col if effective_config.partition_col else 'none',
            'table_location': table_loc,
            'snapshot_id': snapshot_id,
            'batches_processed': batches_processed,
            'new_table_created': conformer.new_table_created,
            'conform_ms': conform_ms,
            'write_ms': write_ms,
            'total_ms': total_ms,
            'casts_applied': casts_applied,
            'casts_lossy': casts_lossy,
            'casts_failed': casts_failed,
        }


def _buffered(batches: Iterable[pa.RecordBatch], size: int) -> Iterator[list[pa.RecordBatch]]:
    """Yields lists of up to `size` batches; the last list may be shorter."""
    buffer: list[pa.RecordBatch] = []
    for batch in batches:
        buffer.append(batch)
        if len(buffer) >= size:
            yield buffer
            buffer = []
    if buffer:
        yield buffer


# Public API functions (thin wrappers)


def load_data_to_iceberg(
    table_data: pa.Table,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    config: LoaderConfig | None = None,
) -> dict[str, Any]:
    """Public wrapper around IcebergLoader.load_data using an optional LoaderConfig."""
    loader = IcebergLoader(catalog, default_config=config)
    return loader.load_data(
        table_data,
        table_identifier,
        config=config,
    )


def load_batches_to_iceberg(
    batch_iterator: Iterator[pa.RecordBatch] | pa.RecordBatchReader,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    config: LoaderConfig | None = None,
) -> dict[str, Any]:
    """Public wrapper around IcebergLoader.load_data_batches using an optional LoaderConfig."""
    loader = IcebergLoader(catalog, default_config=config)
    return loader.load_data_batches(
        batch_iterator,
        table_identifier,
        config,
    )


def load_ipc_stream_to_iceberg(
    stream_source: str | BinaryIO | pa.NativeFile,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    config: LoaderConfig | None = None,
) -> dict[str, Any]:
    """Public wrapper around IcebergLoader.load_ipc_stream using an optional LoaderConfig."""
    loader = IcebergLoader(catalog, default_config=config)
    return loader.load_ipc_stream(
        stream_source,
        table_identifier,
        config,
    )
