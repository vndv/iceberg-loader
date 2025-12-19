from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField

from iceberg_loader.core.partitioning import (
    get_transform_impl,
    is_timestamp_identity,
    parse_partition_transform,
    transform_supports_type,
)
from iceberg_loader.services.logging import logger
from iceberg_loader.utils.types import get_arrow_type, get_iceberg_type


class SchemaManager:
    """
    Responsible for Iceberg table schema operations:
    - Converting Arrow schema to Iceberg schema
    - Creating tables
    - Handling schema evolution
    """

    def __init__(self, catalog: Catalog, table_properties: dict[str, Any]):
        self.catalog = catalog
        self.table_properties = table_properties

    def ensure_table_exists(
        self,
        table_identifier: tuple[str, str],
        arrow_schema: pa.Schema,
        partition_col: str | None = None,
        table_properties: dict[str, Any] | None = None,
    ) -> Any:
        """Loads the table, or creates it if it doesn't exist."""
        try:
            return self.catalog.load_table(table_identifier)
        except (FileNotFoundError, ValueError, NoSuchTableError):
            logger.info('Table %s not found, creating new table.', table_identifier)

            # Pre-process schema: check if partition col needs type adjustment
            adjusted_arrow_schema = self._adjust_schema_for_partitioning(arrow_schema, partition_col)

            iceberg_schema = self._arrow_to_iceberg(adjusted_arrow_schema)
            self._create_table(table_identifier, iceberg_schema, partition_col, table_properties)
            return self.catalog.load_table(table_identifier)

    def evolve_schema_if_needed(self, table: Any, batch_schema: pa.Schema) -> bool:
        """
        Checks if the batch schema contains new columns not present in the table schema.
        If so, creates a schema update transaction to add them.
        """
        current_iceberg_schema = table.schema()
        new_columns = []

        # Convert Arrow schema to Iceberg schema to leverage TypeRegistry
        # We only care about top-level fields for simplicity in this version,
        # but a recursive check would be ideal for nested structs.
        incoming_iceberg_schema = self._arrow_to_iceberg(batch_schema)

        for field in incoming_iceberg_schema.fields:
            if field.name not in current_iceberg_schema.column_names:
                new_columns.append(field)

        if not new_columns:
            return False

        logger.info('Evolving schema: Adding columns %s', [f.name for f in new_columns])
        with table.update_schema() as update:
            for field in new_columns:
                # Add column. The type is already converted to Iceberg type
                update.add_column(field.name, field.field_type, required=False)

        return True

    def get_arrow_schema(self, table: Any) -> pa.Schema:
        """
        Returns the current table schema as a PyArrow schema.
        Crucial for type casting incoming batches to match the table.
        """
        return self._iceberg_to_arrow(table.schema())

    def _create_table(
        self,
        table_identifier: tuple[str, str],
        schema: Schema,
        partition_col: str | None = None,
        properties: dict[str, Any] | None = None,
    ) -> None:
        partition_spec = None
        final_properties = properties if properties is not None else self.table_properties

        if partition_col:
            partition_spec = self._create_partition_spec(schema, partition_col)

        if partition_spec:
            self.catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                partition_spec=partition_spec,
                properties=final_properties,
            )
        else:
            self.catalog.create_table(
                identifier=table_identifier,
                schema=schema,
                properties=final_properties,
            )

    def _adjust_schema_for_partitioning(self, arrow_schema: pa.Schema, partition_col: str | None) -> pa.Schema:
        """
        If partitioning by a time transform (year, month, day, hour) and the source column is String,
        promote it to Timestamp in the schema definition so PyIceberg can handle the transform.
        The data converter (arrow_utils) will later cast string data to Timestamp based on this schema.
        """
        if not partition_col:
            return arrow_schema

        try:
            transform, source_col, _ = parse_partition_transform(partition_col)
        except ValueError:
            return arrow_schema

        if transform in ('year', 'month', 'day', 'hour'):
            idx = arrow_schema.get_field_index(source_col)
            if idx != -1:
                field = arrow_schema.field(idx)
                if pa.types.is_string(field.type) or pa.types.is_large_string(field.type):
                    logger.info(
                        "Promoting partition column '%s' from String to Timestamp(us) to support '%s' transform.",
                        source_col,
                        transform,
                    )
                    # Create a new field with Timestamp type
                    new_field = field.with_type(pa.timestamp('us'))
                    return arrow_schema.set(idx, new_field)

        return arrow_schema

    def _create_partition_spec(self, schema: Schema, partition_col: str) -> PartitionSpec | None:
        """
        Parses the partition string and creates a PartitionSpec.
        """
        try:
            transform, source_col, param = parse_partition_transform(partition_col)
            field = schema.find_field(source_col)

            # Heuristic for new partition field ID: max existing ID + 1
            max_field_id = max(f.field_id for f in schema.fields)
            partition_field_id = max_field_id + 1

            partition_name = self._get_partition_name(transform, source_col, param)
            transform_impl = get_transform_impl(transform, param)

            self._validate_partition_transform(transform, source_col, field.field_type, transform_impl)

            return PartitionSpec(
                PartitionField(
                    source_id=field.field_id,
                    field_id=partition_field_id,
                    transform=transform_impl,
                    name=partition_name,
                ),
            )
        except ValueError as e:
            logger.warning(
                "Failed to create partition spec for '%s': %s. Creating table without partition.",
                partition_col,
                e,
            )
            return None

    def _get_partition_name(self, transform: str, source_col: str, param: int | None) -> str:
        if transform == 'identity':
            return source_col
        if transform == 'bucket':
            return f'{source_col}_bucket_{param}'
        if transform == 'truncate':
            return f'{source_col}_trunc_{param}'
        if transform == 'void':
            return f'{source_col}_void'
        return f'{source_col}_{transform}'

    def _validate_partition_transform(
        self,
        transform: str,
        source_col: str,
        field_type: Any,
        transform_impl: Any,
    ) -> None:
        if is_timestamp_identity(transform, field_type):
            logger.warning(
                "Identity partition on timestamp column '%s' can create too many partitions. "
                'Use day(...) or hour(...).',
                source_col,
            )

        if not transform_impl.can_transform(field_type) and not transform_supports_type(transform, field_type):
            logger.warning(
                "Transform '%s' may not support type '%s' for column '%s'. Partitioning might fail.",
                transform,
                field_type,
                source_col,
            )

    def _arrow_to_iceberg(self, arrow_schema: pa.Schema, existing_schema: Schema | None = None) -> Schema:
        """
        Converts PyArrow schema to PyIceberg schema.
        If existing_schema is provided, preserves field IDs from it.
        """
        fields = []
        # Simple ID generator for new fields if no existing schema
        # If existing schema, we need to find max ID.
        next_id = 1
        if existing_schema:
            if existing_schema.fields:
                next_id = max(f.field_id for f in existing_schema.fields) + 1
        else:
            # Start fresh
            next_id = 1

        for field in arrow_schema:
            field_id = -1
            if existing_schema:
                try:
                    existing_field = existing_schema.find_field(field.name)
                    field_id = existing_field.field_id
                except ValueError:
                    pass

            if field_id == -1:
                field_id = next_id
                next_id += 1

            # Use TypeRegistry for conversion
            iceberg_type = get_iceberg_type(field.type)

            fields.append(
                NestedField(
                    field_id=field_id,
                    name=field.name,
                    field_type=iceberg_type,
                    required=not field.nullable,
                ),
            )

        return Schema(*fields)

    def _iceberg_to_arrow(self, iceberg_schema: Schema) -> pa.Schema:
        """Converts PyIceberg schema back to PyArrow schema for casting."""
        fields = []
        for field in iceberg_schema.fields:
            arrow_type = get_arrow_type(field.field_type)
            fields.append(pa.field(field.name, arrow_type, nullable=not field.required))
        return pa.schema(fields)
