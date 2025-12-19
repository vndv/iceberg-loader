from collections.abc import Iterator
from typing import Any, BinaryIO

import pyarrow as pa
from pyiceberg.catalog import Catalog

from iceberg_loader.core.config import TABLE_PROPERTIES, LoaderConfig, ensure_loader_config
from iceberg_loader.core.schema import SchemaManager
from iceberg_loader.core.strategies import get_write_strategy
from iceberg_loader.services.logging import logger
from iceberg_loader.utils.arrow import convert_table_types


class IcebergLoader:
    """
    Facade for loading data into Iceberg tables.
    Orchestrates SchemaManager and WriteStrategy to handle complex ingestion scenarios.
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

    def _normalize_batches(
        self,
        batches: list[pa.RecordBatch],
        table: Any | None,
        tid: tuple[str, str],
        config: LoaderConfig,
        table_props: dict[str, Any],
    ) -> tuple[pa.Table, Any, bool]:
        """
        Normalizes mixed-schema batches into a single PyArrow Table.
        Returns (combined_table, updated_iceberg_table, was_new_table_created).
        """
        new_table_created = False
        # If table doesn't exist yet, create it from the first batch's schema
        if table is None:
            table = self.schema_manager.ensure_table_exists(
                tid,
                batches[0].schema,
                config.partition_col,
                table_properties=table_props,
            )
            if table.current_snapshot() is None:
                new_table_created = True

        # Evolve schema for all batches
        for b in batches:
            self.schema_manager.evolve_schema_if_needed(table, b.schema)

        target_arrow_schema = self.schema_manager.get_arrow_schema(table)

        # Cast all batches
        normalized_tables = []
        for b in batches:
            t = pa.Table.from_batches([b])
            t_casted = convert_table_types(t, target_arrow_schema)
            normalized_tables.append(t_casted)

        return pa.concat_tables(normalized_tables), table, new_table_created

    def _process_batch_buffer(
        self,
        batches: list[pa.RecordBatch],
        context: dict[str, Any],
        config: LoaderConfig,
        table_props: dict[str, Any],
        tid: tuple[str, str],
        strategy: Any,
    ) -> None:
        """Processes a buffer of batches: normalizes schema, evolves table, writes data."""
        if not batches:
            return

        table = context['table']
        combined_table = None

        if config.schema_evolution:
            try:
                combined_table = pa.Table.from_batches(batches)
            except pa.ArrowInvalid:
                logger.info('Mixed schemas in batch buffer. Normalizing...')
                combined_table, table, created = self._normalize_batches(batches, table, tid, config, table_props)
                if created:
                    context['new_table_created'] = True

        if combined_table is None:
            combined_table = pa.Table.from_batches(batches)

        # Add load timestamp if configured
        if config.load_timestamp:
            timestamp_array = pa.array(
                [config.load_timestamp] * len(combined_table),
                type=pa.timestamp('us'),
            )
            combined_table = combined_table.append_column(config.load_ts_col, timestamp_array)

        # 1. Ensure Table Exists
        if table is None:
            table = self.schema_manager.ensure_table_exists(
                tid,
                combined_table.schema,
                config.partition_col,
                table_properties=table_props,
            )
            if table.current_snapshot() is None:
                context['new_table_created'] = True

        # 1.5 Force schema evolution for load timestamp
        if config.load_timestamp:
            ts_field = combined_table.schema.field(config.load_ts_col)
            mini_schema = pa.schema([ts_field])
            self.schema_manager.evolve_schema_if_needed(table, mini_schema)

        # 2. Schema Evolution
        if config.schema_evolution:
            self.schema_manager.evolve_schema_if_needed(table, combined_table.schema)

        # 3. Type Conversion
        target_schema = self.schema_manager.get_arrow_schema(table)
        combined_table = convert_table_types(combined_table, target_schema)

        # The strategy now handles transaction management internally
        strategy.write(table, combined_table, context['is_first_write'])

        # Update context
        context['table'] = table
        context['is_first_write'] = False
        context['total_rows'] += len(combined_table)

    def load_data_batches(
        self,
        batch_iterator: Iterator[pa.RecordBatch] | pa.RecordBatchReader,
        table_identifier: tuple[str, str],
        config: LoaderConfig | None = None,
    ) -> dict[str, Any]:
        """
        Main orchestration method.
        Iterates over batches, manages buffers, and delegates writing.
        """
        batches_processed = 0
        pending_batches: list[pa.RecordBatch] = []

        effective_config = self._resolve_config(config)
        effective_table_properties = self.table_properties.copy()
        if effective_config.table_properties:
            effective_table_properties.update(effective_config.table_properties)

        strategy = get_write_strategy(
            effective_config.write_mode,
            effective_config.replace_filter,
            effective_config.join_cols,
        )

        # State tracking context
        context = {
            'table': None,
            'is_first_write': True,
            'new_table_created': False,
            'total_rows': 0,
        }

        # Main Loop
        for batch in batch_iterator:
            pending_batches.append(batch)
            batches_processed += 1
            limit = max(1, effective_config.commit_interval)

            if len(pending_batches) >= limit:
                self._process_batch_buffer(
                    pending_batches,
                    context,
                    effective_config,
                    effective_table_properties,
                    table_identifier,
                    strategy,
                )
                pending_batches = []

        if pending_batches:
            self._process_batch_buffer(
                pending_batches,
                context,
                effective_config,
                effective_table_properties,
                table_identifier,
                strategy,
            )

        table: Any = context['table']
        # 'table' could be None if no batches were processed at all.
        # But if batches were processed, 'table' should be set.
        # If table is None, we return 'none' for properties.
        snapshot_id = 'none'
        table_loc = 'none'

        if table is not None:
            table_loc = table.location()
            current_snap = table.current_snapshot()
            if current_snap:
                snapshot_id = current_snap.snapshot_id

        return {
            'rows_loaded': context['total_rows'],
            'write_mode': effective_config.write_mode,
            'partition_col': effective_config.partition_col if effective_config.partition_col else 'none',
            'table_location': table_loc,
            'snapshot_id': snapshot_id,
            'batches_processed': batches_processed,
            'new_table_created': context['new_table_created'],
        }


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
