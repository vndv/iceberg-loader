import logging
import re
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Transform,
    TruncateTransform,
    YearTransform,
)
from pyiceberg.types import (
    DateType,
    NestedField,
    TimestampType,
    TimestamptzType,
)

from iceberg_loader.type_mappings import get_arrow_type, get_iceberg_type

logger = logging.getLogger(__name__)


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
    ) -> Any:
        """Loads the table, or creates it if it doesn't exist."""
        try:
            return self.catalog.load_table(table_identifier)
        except (FileNotFoundError, ValueError, NoSuchTableError):
            logger.info('Table %s not found, creating new table.', table_identifier)

            # Pre-process schema: check if partition col needs type adjustment
            adjusted_arrow_schema = self._adjust_schema_for_partitioning(arrow_schema, partition_col)

            iceberg_schema = self._arrow_to_iceberg(adjusted_arrow_schema)
            self._create_table(table_identifier, iceberg_schema, partition_col)
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
    ) -> None:
        partition_spec = None

        if partition_col:
            partition_spec = self._create_partition_spec(schema, partition_col)

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

    def _adjust_schema_for_partitioning(self, arrow_schema: pa.Schema, partition_col: str | None) -> pa.Schema:
        """
        If partitioning by a time transform (year, month, day, hour) and the source column is String,
        promote it to Timestamp in the schema definition so PyIceberg can handle the transform.
        The data converter (arrow_utils) will later cast string data to Timestamp based on this schema.
        """
        if not partition_col:
            return arrow_schema

        try:
            transform, source_col, _ = self._parse_partition_transform(partition_col)
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
        Supports:
        - "col" (Identity)
        - "year(col)", "month(col)", "day(col)", "hour(col)"
        - "bucket(N, col)"
        - "truncate(W, col)"
        """
        try:
            transform, source_col, param = self._parse_partition_transform(partition_col)
            field = schema.find_field(source_col)

            # Heuristic for new partition field ID: max existing ID + 1
            max_field_id = max(f.field_id for f in schema.fields)
            partition_field_id = max_field_id + 1

            partition_name = f'{source_col}_{transform}' if transform != 'identity' else source_col
            if transform == 'bucket':
                partition_name = f'{source_col}_bucket_{param}'
            elif transform == 'truncate':
                partition_name = f'{source_col}_trunc_{param}'
            elif transform == 'void':
                partition_name = f'{source_col}_void'

            transform_impl = self._get_transform_impl(transform, param)

            # Validation: Check if transform supports the source type
            if not transform_impl.can_transform(field.field_type):
                # If we promoted schema in _adjust_schema_for_partitioning, this should pass.
                # If not, try to be helpful.
                if transform in ('year', 'month', 'day', 'hour') and isinstance(
                    field.field_type, DateType | TimestampType | TimestamptzType
                ):
                    pass  # Good
                else:
                    logger.warning(
                        "Transform '%s' may not support type '%s' for column '%s'. Partitioning might fail.",
                        transform,
                        field.field_type,
                        source_col,
                    )

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
                "Failed to create partition spec for '%s': %s. Creating table without partition.", partition_col, e
            )
            return None

    def _parse_partition_transform(self, partition_str: str) -> tuple[str, str, int | None]:
        """
        Parses strings like:
        - "ts" -> ("identity", "ts", None)
        - "month(ts)" -> ("month", "ts", None)
        - "bucket(16, id)" -> ("bucket", "id", 16)
        - "truncate(4, name)" -> ("truncate", "name", 4)
        """
        partition_str = partition_str.strip()

        # Regex for "func(col)" or "func(param, col)"
        # Group 1: Function name (optional)
        # Group 2: Args inside parens
        match = re.match(r'(\w+)\((.+)\)', partition_str)

        if not match:
            # Identity case: "ts"
            return 'identity', partition_str, None

        func_name = match.group(1).lower()
        args_str = match.group(2)
        args = [a.strip() for a in args_str.split(',')]

        if func_name in ('year', 'month', 'day', 'hour', 'void'):
            if len(args) != 1:
                raise ValueError(f"Transform '{func_name}' expects 1 argument, got {len(args)}")
            return func_name, args[0], None

        if func_name in ('bucket', 'truncate'):
            if len(args) != 2:
                raise ValueError(f"Transform '{func_name}' expects 2 arguments (param, col), got {len(args)}")
            try:
                param = int(args[0])
            except ValueError as e:
                raise ValueError(f"First argument for '{func_name}' must be an integer, got '{args[0]}'") from e
            return func_name, args[1], param

        # Fallback or unknown transform, treat as identity if simple column?
        # Or strict error? Let's raise error for unknown transforms.
        raise ValueError(f'Unknown partition transform: {func_name}')

    def _get_transform_impl(self, transform_name: str, param: int | None) -> Transform:
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
        # Void not commonly used but supported
        raise ValueError(f'Unsupported transform implementation: {transform_name}')

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
                )
            )

        return Schema(*fields)

    def _iceberg_to_arrow(self, iceberg_schema: Schema) -> pa.Schema:
        """Converts PyIceberg schema back to PyArrow schema for casting."""
        fields = []
        for field in iceberg_schema.fields:
            arrow_type = get_arrow_type(field.field_type)
            fields.append(pa.field(field.name, arrow_type, nullable=not field.required))
        return pa.schema(fields)
