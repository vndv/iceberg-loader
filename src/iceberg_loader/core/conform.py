from collections.abc import Iterable
from typing import Any

import pyarrow as pa

from iceberg_loader.core.config import LoaderConfig
from iceberg_loader.core.schema import SchemaManager
from iceberg_loader.services.logging import logger
from iceberg_loader.utils.arrow import CastStats, convert_table_types


class BatchConformer:
    """
    Turns buffers of record batches into PyArrow tables that fit one Iceberg table.

    One instance serves one load. The first conform() call loads the target table, or creates it
    with the configured partition spec and properties. Every call then:
    - appends the load timestamp column (if configured) before the table is created or evolved
    - adds the load timestamp column to the table schema, even when schema_evolution is off
    - with schema_evolution, adds new top-level columns found in any batch
    - casts to the table schema: unknown columns are dropped, missing columns become NULL,
      values that cannot be cast become NULL with a warning

    Without schema_evolution, a buffer with mixed schemas raises pa.ArrowInvalid before any table is created.

    requested_format_version is the format-version the user set explicitly (None when only the library
    default applies). If the table already exists with a different version, a warning is logged and the
    table keeps its version.
    """

    def __init__(
        self,
        schema_manager: SchemaManager,
        table_identifier: tuple[str, str],
        config: LoaderConfig,
        table_properties: dict[str, Any],
        requested_format_version: Any = None,
    ):
        self._schema_manager = schema_manager
        self._table_identifier = table_identifier
        self._config = config
        self._table_properties = table_properties
        self._requested_format_version = requested_format_version
        self.table: Any | None = None
        self.new_table_created = False
        self.cast_stats = CastStats()

    def conform(self, batches: list[pa.RecordBatch]) -> pa.Table:
        if not batches:
            raise ValueError('conform() needs at least one batch')

        tables = [self._with_load_timestamp(pa.Table.from_batches([b])) for b in batches]
        if not self._config.schema_evolution:
            # Mixed schemas raise pa.ArrowInvalid here, before any table is created.
            tables = [pa.concat_tables(tables)]

        table = self._ensure_table(tables[0].schema)

        if self._config.load_timestamp:
            ts_field = pa.field(self._config.load_ts_col, pa.timestamp('us'))
            self._schema_manager.evolve_schema_if_needed(table, pa.schema([ts_field]))

        if self._config.schema_evolution:
            schemas = _distinct_schemas(t.schema for t in tables)
            if len(schemas) > 1:
                logger.info('Mixed schemas in batch buffer. Normalizing...')
            for schema in schemas:
                self._schema_manager.evolve_schema_if_needed(table, schema)

        target_schema = self._schema_manager.get_arrow_schema(table)
        return pa.concat_tables([convert_table_types(t, target_schema, self.cast_stats) for t in tables])

    def _with_load_timestamp(self, data: pa.Table) -> pa.Table:
        if not self._config.load_timestamp:
            return data
        timestamps = pa.array([self._config.load_timestamp] * len(data), type=pa.timestamp('us'))
        return data.append_column(self._config.load_ts_col, timestamps)

    def _ensure_table(self, arrow_schema: pa.Schema) -> Any:
        if self.table is None:
            self.table = self._schema_manager.ensure_table_exists(
                self._table_identifier,
                arrow_schema,
                self._config.partition_col,
                table_properties=self._table_properties,
                requested_format_version=self._requested_format_version,
            )
            if self.table.current_snapshot() is None:
                self.new_table_created = True
        return self.table


def _distinct_schemas(schemas: Iterable[pa.Schema]) -> list[pa.Schema]:
    distinct: list[pa.Schema] = []
    for schema in schemas:
        if not any(schema.equals(seen) for seen in distinct):
            distinct.append(schema)
    return distinct
