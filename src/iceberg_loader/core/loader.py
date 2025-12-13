from collections.abc import Iterator
from typing import Any, BinaryIO

import pyarrow as pa
from pyiceberg.catalog import Catalog

from iceberg_loader.core.config import TABLE_PROPERTIES, LoaderConfig
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
        self.default_config = default_config or LoaderConfig()

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
        Iterates over batches, manages buffers, and delegates writing.
        """
        total_rows = 0
        batches_processed = 0

        # Buffer
        pending_batches: list[pa.RecordBatch] = []

        # Resolve config
        effective_config = config or self.default_config

        # Merge global loader properties with config-specific properties
        effective_table_properties = self.table_properties.copy()
        if effective_config.table_properties:
            effective_table_properties.update(effective_config.table_properties)

        strategy = get_write_strategy(
            effective_config.write_mode,
            effective_config.replace_filter,
            effective_config.join_cols,
        )
        effective_schema_evolution = effective_config.schema_evolution

        # State tracking
        table = None
        is_first_write = True
        new_table_created = False

        def process_buffer(batches: list[pa.RecordBatch]) -> None:
            nonlocal table, new_table_created, is_first_write, total_rows

            if not batches:
                return

            combined_table = None

            if effective_schema_evolution:
                # Try fast path
                try:
                    combined_table = pa.Table.from_batches(batches)
                except pa.ArrowInvalid:
                    # Mixed schemas in buffer
                    logger.info('Mixed schemas in batch buffer. Normalizing...')

                    if table is None:
                        table = self.schema_manager.ensure_table_exists(
                            table_identifier,
                            batches[0].schema,
                            effective_config.partition_col,
                            table_properties=effective_table_properties,
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

                    combined_table = pa.concat_tables(normalized_tables)

            if combined_table is None:
                combined_table = pa.Table.from_batches(batches)

            # Add load timestamp if configured
            if effective_config.load_timestamp:
                timestamp_array = pa.array(
                    [effective_config.load_timestamp] * len(combined_table),
                    type=pa.timestamp('us'),
                )
                combined_table = combined_table.append_column(effective_config.load_ts_col, timestamp_array)

            # 1. Ensure Table Exists
            if table is None:
                table = self.schema_manager.ensure_table_exists(
                    table_identifier,
                    combined_table.schema,
                    effective_config.partition_col,
                    table_properties=effective_table_properties,
                )
                if table.current_snapshot() is None:
                    new_table_created = True

            # 1.5 Force schema evolution for load timestamp
            if effective_config.load_timestamp:
                ts_field = combined_table.schema.field(effective_config.load_ts_col)
                mini_schema = pa.schema([ts_field])
                self.schema_manager.evolve_schema_if_needed(table, mini_schema)

            # 2. Schema Evolution
            if effective_schema_evolution:
                self.schema_manager.evolve_schema_if_needed(table, combined_table.schema)

            # 3. Type Conversion
            target_schema = self.schema_manager.get_arrow_schema(table)
            combined_table = convert_table_types(combined_table, target_schema)

            # The strategy now handles transaction management internally
            strategy.write(table, combined_table, is_first_write)

            is_first_write = False
            total_rows += len(combined_table)

        # Main Loop
        for batch in batch_iterator:
            pending_batches.append(batch)
            batches_processed += 1

            limit = 1 if effective_config.commit_interval <= 1 else effective_config.commit_interval

            if len(pending_batches) >= limit:
                process_buffer(pending_batches)
                pending_batches = []

        # Flush remainder
        if pending_batches:
            process_buffer(pending_batches)

        return {
            'rows_loaded': total_rows,
            'write_mode': effective_config.write_mode,
            'partition_col': effective_config.partition_col if effective_config.partition_col else 'none',
            'table_location': table.location() if table else 'none',
            'snapshot_id': table.current_snapshot().snapshot_id if table and table.current_snapshot() else 'none',
            'batches_processed': batches_processed,
            'new_table_created': new_table_created,
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
