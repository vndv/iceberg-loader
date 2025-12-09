import logging
from collections.abc import Iterator
from itertools import count
from typing import Any, BinaryIO, Literal

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import NestedField

from iceberg_loader.arrow_utils import convert_table_types
from iceberg_loader.settings import TABLE_PROPERTIES
from iceberg_loader.type_mappings import get_arrow_type, get_iceberg_type

logger = logging.getLogger(__name__)


class IcebergLoader:
    """
    Loader for Apache Iceberg tables with support for complex JSON data.
    
    Handles schema inference, type conversion, partitioning, and various write modes
    (overwrite, append, idempotent). Automatically serializes complex types to JSON strings.
    """

    def __init__(self, catalog: Catalog, table_properties: dict[str, Any] | None = None):
        self.catalog = catalog
        self.table_properties = TABLE_PROPERTIES.copy()
        if table_properties:
            self.table_properties.update(table_properties)

    def _convert_arrow_to_iceberg_schema(self, arrow_schema: pa.Schema) -> Schema:
        field_id_counter = count(1)
        fields = []

        for field in arrow_schema:
            iceberg_type = get_iceberg_type(field.type)
            iceberg_field = NestedField(
                field_id=next(field_id_counter),
                name=field.name,
                field_type=iceberg_type,
                required=not field.nullable,
            )
            fields.append(iceberg_field)

        return Schema(*fields)

    def _create_iceberg_table(
        self,
        table_identifier: tuple[str, str],
        schema: Schema,
        partition_col: str | None = None,
    ) -> None:
        partition_spec = None

        if partition_col:
            try:
                field = schema.find_field(partition_col)
                max_field_id = max(f.field_id for f in schema.fields)
                partition_field_id = max_field_id + 1

                partition_spec = PartitionSpec(
                    PartitionField(
                        source_id=field.field_id,
                        field_id=partition_field_id,
                        transform=IdentityTransform(),
                        name=f'{partition_col}_partition',
                    ),
                )
            except ValueError:
                logger.warning(
                    "Partition column '%s' not found in schema. Creating table without partition.", partition_col
                )

        if partition_spec:
            self.catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=self.table_properties,
            )
        else:
            self.catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                properties=self.table_properties,
            )

    def _convert_iceberg_schema_to_arrow(self, schema: Schema) -> pa.Schema:
        fields = [
            pa.field(field.name, get_arrow_type(field.field_type), nullable=not field.required)
            for field in schema.fields
        ]
        return pa.schema(fields)

    def _load_table_if_exists(self, table_identifier: tuple[str, str]) -> Any | None:
        try:
            return self.catalog.load_table(table_identifier)
        except (FileNotFoundError, ValueError, NoSuchTableError):
            return None

    def load_data(
        self,
        table_data: pa.Table,
        table_identifier: tuple[str, str],
        write_mode: Literal['overwrite', 'append'] = 'overwrite',
        partition_col: str | None = None,
        replace_filter: str | None = None,
        schema_evolution: bool = False,
    ) -> dict[str, Any]:
        """
        Load PyArrow Table into Iceberg table with automatic schema handling.
        
        Creates table if it doesn't exist, converts types to match Iceberg schema,
        and supports overwrite/append modes with optional idempotent writes.
        """
        table = self._load_table_if_exists(table_identifier)
        created_new_table = False

        if table is None:
            schema = self._convert_arrow_to_iceberg_schema(table_data.schema)
            self._create_iceberg_table(table_identifier, schema, partition_col=partition_col)
            table = self.catalog.load_table(table_identifier)
            created_new_table = True
        else:
            # Only check for evolution if we didn't just create the table
            if schema_evolution:
                logger.info('Checking for schema evolution...')
                new_schema = self._convert_arrow_to_iceberg_schema(table_data.schema)
                with table.update_schema() as update:
                    update.union_by_name(new_schema)
                table.refresh()

        iceberg_schema = table.schema()
        arrow_schema = self._convert_iceberg_schema_to_arrow(iceberg_schema)
        table_data = convert_table_types(table_data, arrow_schema)

        with table.transaction() as txn:
            if write_mode == 'overwrite':
                txn.overwrite(table_data)
            else:
                if replace_filter:
                    logger.info('Implementing idempotent load: deleting rows matching filter: %s', replace_filter)
                    txn.delete(replace_filter)
                txn.append(table_data)

        # Maintenance tasks (expire_snapshots) should be run separately
        # self.maintenance.expire_snapshots(table)

        return {
            'rows_loaded': len(table_data),
            'write_mode': write_mode,
            'partition_col': partition_col if partition_col else 'none',
            'table_location': table.location(),
            'snapshot_id': table.current_snapshot().snapshot_id if table.current_snapshot() else 'none',
            'new_table_created': created_new_table,
        }

    def load_ipc_stream(
        self,
        stream_source: str | BinaryIO | pa.NativeFile,
        table_identifier: tuple[str, str],
        write_mode: Literal['overwrite', 'append'] = 'overwrite',
        partition_col: str | None = None,
        replace_filter: str | None = None,
        schema_evolution: bool = False,
    ) -> dict[str, Any]:
        """
        Loads data from an Apache Arrow IPC stream source.
        """
        with pa.ipc.open_stream(stream_source) as reader:
            return self.load_data_batches(
                batch_iterator=reader,
                table_identifier=table_identifier,
                write_mode=write_mode,
                partition_col=partition_col,
                replace_filter=replace_filter,
                schema_evolution=schema_evolution,
            )

    def load_data_batches(
        self,
        batch_iterator: Iterator[pa.RecordBatch] | pa.RecordBatchReader,
        table_identifier: tuple[str, str],
        write_mode: Literal['overwrite', 'append'] = 'overwrite',
        partition_col: str | None = None,
        replace_filter: str | None = None,
        schema_evolution: bool = False,
    ) -> dict[str, Any]:
        """
        Load data from RecordBatch iterator/reader into Iceberg table.
        
        Efficiently processes streaming data in batches, creating table if needed
        and handling schema evolution. Supports overwrite/append with idempotent writes.
        """
        total_rows = 0
        first_batch = None

        table = self._load_table_if_exists(table_identifier)
        created_new_table = False

        try:
            first_batch = next(batch_iterator)
            total_rows += len(first_batch)
        except StopIteration:
            return {
                'rows_loaded': 0,
                'write_mode': write_mode,
                'batches_processed': 0,
            }

        if table is None:
            temp_table = pa.Table.from_batches([first_batch])
            schema = self._convert_arrow_to_iceberg_schema(temp_table.schema)
            self._create_iceberg_table(table_identifier, schema, partition_col=partition_col)
            table = self.catalog.load_table(table_identifier)
            created_new_table = True

        # Optimization: Don't evolve if just created
        if schema_evolution and first_batch is not None and not created_new_table:
            logger.info('Checking for schema evolution based on first batch...')
            temp_table = pa.Table.from_batches([first_batch])
            new_schema = self._convert_arrow_to_iceberg_schema(temp_table.schema)
            with table.update_schema() as update:
                update.union_by_name(new_schema)
            table.refresh()

        iceberg_schema = table.schema()
        arrow_schema = self._convert_iceberg_schema_to_arrow(iceberg_schema)

        with table.transaction() as txn:
            if first_batch is not None:
                first_table = pa.Table.from_batches([first_batch])
                first_table = convert_table_types(first_table, arrow_schema)

                if write_mode == 'overwrite':
                    txn.overwrite(first_table)
                else:
                    if replace_filter:
                        logger.info('Implementing idempotent load: deleting rows matching filter: %s', replace_filter)
                        txn.delete(replace_filter)
                    txn.append(first_table)

            batches_processed = 1
            for batch in batch_iterator:
                logger.info(
                    'Processing batch %s with size %s. Table: %s',
                    batches_processed,
                    len(batch),
                    table_identifier,
                )

                batch_table = pa.Table.from_batches([batch])
                batch_table = convert_table_types(batch_table, arrow_schema)

                txn.append(batch_table)

                total_rows += len(batch)
                batches_processed += 1

        # Maintenance tasks (expire_snapshots) should be run separately
        # self.maintenance.expire_snapshots(table)

        return {
            'rows_loaded': total_rows,
            'write_mode': write_mode,
            'partition_col': partition_col if partition_col else 'none',
            'table_location': table.location(),
            'snapshot_id': table.current_snapshot().snapshot_id if table.current_snapshot() else 'none',
            'batches_processed': batches_processed,
        }


def load_data_to_iceberg(
    table_data: pa.Table,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    write_mode: Literal['overwrite', 'append'] = 'overwrite',
    # New flexible parameters
    partition_col: str | None = None,
    replace_filter: str | None = None,
    schema_evolution: bool = False,
    table_properties: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Public API wrapper for loading PyArrow Table into Iceberg table."""
    loader = IcebergLoader(catalog, table_properties)
    return loader.load_data(table_data, table_identifier, write_mode, partition_col, replace_filter, schema_evolution)


def load_batches_to_iceberg(
    batch_iterator: Iterator[pa.RecordBatch] | pa.RecordBatchReader,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    write_mode: Literal['overwrite', 'append'] = 'overwrite',
    # New flexible parameters
    partition_col: str | None = None,
    replace_filter: str | None = None,
    schema_evolution: bool = False,
    table_properties: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Public API wrapper for loading RecordBatch iterator/reader into Iceberg table."""
    loader = IcebergLoader(catalog, table_properties)
    return loader.load_data_batches(
        batch_iterator, table_identifier, write_mode, partition_col, replace_filter, schema_evolution
    )


def load_ipc_stream_to_iceberg(
    stream_source: str | BinaryIO | pa.NativeFile,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    write_mode: Literal['overwrite', 'append'] = 'overwrite',
    partition_col: str | None = None,
    replace_filter: str | None = None,
    schema_evolution: bool = False,
    table_properties: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Public API wrapper for loading Apache Arrow IPC stream into Iceberg table."""
    loader = IcebergLoader(catalog, table_properties)
    return loader.load_ipc_stream(
        stream_source, table_identifier, write_mode, partition_col, replace_filter, schema_evolution
    )
