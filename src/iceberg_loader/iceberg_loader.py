import logging
from collections.abc import Iterator
from typing import Any, BinaryIO, Literal

import pyarrow as pa
from pyiceberg.catalog import Catalog

from iceberg_loader.arrow_utils import convert_table_types
from iceberg_loader.schema import SchemaManager
from iceberg_loader.settings import TABLE_PROPERTIES
from iceberg_loader.strategies import get_write_strategy

logger = logging.getLogger(__name__)


class IcebergLoader:
    """
    Facade for loading data into Iceberg tables.
    Orchestrates SchemaManager and WriteStrategy to handle complex ingestion scenarios.
    """

    def __init__(self, catalog: Catalog, table_properties: dict[str, Any] | None = None):
        self.catalog = catalog
        self.table_properties = TABLE_PROPERTIES.copy()
        if table_properties:
            self.table_properties.update(table_properties)

        self.schema_manager = SchemaManager(self.catalog, self.table_properties)

    def load_data(
        self,
        table_data: pa.Table,
        table_identifier: tuple[str, str],
        write_mode: Literal['overwrite', 'append', 'upsert'] = 'overwrite',
        partition_col: str | None = None,
        replace_filter: str | None = None,
        schema_evolution: bool = False,
        join_cols: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Load PyArrow Table into Iceberg table.
        Delegates to load_data_batches for consistency.
        """
        batches = table_data.to_batches()
        return self.load_data_batches(
            batch_iterator=iter(batches),
            table_identifier=table_identifier,
            write_mode=write_mode,
            partition_col=partition_col,
            replace_filter=replace_filter,
            schema_evolution=schema_evolution,
            join_cols=join_cols,
        )

    def load_ipc_stream(
        self,
        stream_source: str | BinaryIO | pa.NativeFile,
        table_identifier: tuple[str, str],
        write_mode: Literal['overwrite', 'append', 'upsert'] = 'overwrite',
        partition_col: str | None = None,
        replace_filter: str | None = None,
        schema_evolution: bool = False,
        commit_interval: int = 0,
        join_cols: list[str] | None = None,
    ) -> dict[str, Any]:
        """Loads data from an Apache Arrow IPC stream source."""
        with pa.ipc.open_stream(stream_source) as reader:
            return self.load_data_batches(
                batch_iterator=reader,
                table_identifier=table_identifier,
                write_mode=write_mode,
                partition_col=partition_col,
                replace_filter=replace_filter,
                schema_evolution=schema_evolution,
                commit_interval=commit_interval,
                join_cols=join_cols,
            )

    def load_data_batches(
        self,
        batch_iterator: Iterator[pa.RecordBatch] | pa.RecordBatchReader,
        table_identifier: tuple[str, str],
        write_mode: Literal['overwrite', 'append', 'upsert'] = 'overwrite',
        partition_col: str | None = None,
        replace_filter: str | None = None,
        schema_evolution: bool = False,
        commit_interval: int = 0,
        join_cols: list[str] | None = None,
    ) -> dict[str, Any]:
        """
        Main orchestration method.
        Iterates over batches, manages buffers, and delegates writing.
        """
        total_rows = 0
        batches_processed = 0

        # Buffer
        pending_batches: list[pa.RecordBatch] = []

        # Strategy selection
        strategy = get_write_strategy(write_mode, replace_filter, join_cols)

        # State tracking
        table = None
        is_first_write = True
        new_table_created = False

        def process_buffer(batches: list[pa.RecordBatch]) -> None:
            nonlocal table, new_table_created, is_first_write, total_rows

            if not batches:
                return

            combined_table = None

            if schema_evolution:
                # Try fast path
                try:
                    combined_table = pa.Table.from_batches(batches)
                except pa.ArrowInvalid:
                    # Mixed schemas in buffer
                    logger.info('Mixed schemas in batch buffer. Normalizing...')

                    if table is None:
                        # Use first batch to ensure table exists
                        table = self.schema_manager.ensure_table_exists(
                            table_identifier, batches[0].schema, partition_col
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

            # 1. Ensure Table Exists
            if table is None:
                table = self.schema_manager.ensure_table_exists(table_identifier, combined_table.schema, partition_col)
                if table.current_snapshot() is None:
                    new_table_created = True

            # 2. Schema Evolution
            if schema_evolution:
                self.schema_manager.evolve_schema_if_needed(table, combined_table.schema)

            # 3. Type Conversion
            target_schema = self.schema_manager.get_arrow_schema(table)
            combined_table = convert_table_types(combined_table, target_schema)

            # 4. Write via Strategy
            # The strategy now handles transaction management internally
            strategy.write(table, combined_table, is_first_write)

            is_first_write = False
            total_rows += len(combined_table)

        # Main Loop
        for batch in batch_iterator:
            pending_batches.append(batch)
            batches_processed += 1

            limit = 1 if commit_interval <= 1 else commit_interval

            if len(pending_batches) >= limit:
                process_buffer(pending_batches)
                pending_batches = []

        # Flush remainder
        if pending_batches:
            process_buffer(pending_batches)

        return {
            'rows_loaded': total_rows,
            'write_mode': write_mode,
            'partition_col': partition_col if partition_col else 'none',
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
    write_mode: Literal['overwrite', 'append', 'upsert'] = 'overwrite',
    partition_col: str | None = None,
    replace_filter: str | None = None,
    schema_evolution: bool = False,
    table_properties: dict[str, Any] | None = None,
    join_cols: list[str] | None = None,
) -> dict[str, Any]:
    loader = IcebergLoader(catalog, table_properties)
    return loader.load_data(
        table_data,
        table_identifier,
        write_mode,
        partition_col,
        replace_filter,
        schema_evolution,
        join_cols,
    )


def load_batches_to_iceberg(
    batch_iterator: Iterator[pa.RecordBatch] | pa.RecordBatchReader,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    write_mode: Literal['overwrite', 'append', 'upsert'] = 'overwrite',
    partition_col: str | None = None,
    replace_filter: str | None = None,
    schema_evolution: bool = False,
    table_properties: dict[str, Any] | None = None,
    commit_interval: int = 0,
    join_cols: list[str] | None = None,
) -> dict[str, Any]:
    loader = IcebergLoader(catalog, table_properties)
    return loader.load_data_batches(
        batch_iterator,
        table_identifier,
        write_mode,
        partition_col,
        replace_filter,
        schema_evolution,
        commit_interval,
        join_cols,
    )


def load_ipc_stream_to_iceberg(
    stream_source: str | BinaryIO | pa.NativeFile,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    write_mode: Literal['overwrite', 'append', 'upsert'] = 'overwrite',
    partition_col: str | None = None,
    replace_filter: str | None = None,
    schema_evolution: bool = False,
    table_properties: dict[str, Any] | None = None,
    commit_interval: int = 0,
    join_cols: list[str] | None = None,
) -> dict[str, Any]:
    loader = IcebergLoader(catalog, table_properties)
    return loader.load_ipc_stream(
        stream_source,
        table_identifier,
        write_mode,
        partition_col,
        replace_filter,
        schema_evolution,
        commit_interval,
        join_cols,
    )
