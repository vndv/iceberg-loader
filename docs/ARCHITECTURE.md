# Architecture

How `iceberg-loader` moves Arrow data through table setup, schema handling, and write strategies.

## Components

| Component | Module | Role |
|-----------|--------|------|
| Public API | `iceberg_loader/__init__.py` | Re-exports `LoaderConfig`, `IcebergLoader`, the `load_*_to_iceberg` functions, `register_parquet_files`, `get_rest_catalog`, `expire_snapshots`, and the module-level `logger`. |
| `load_*_to_iceberg` | `core/loader.py` | Thin wrappers: each call builds a new `IcebergLoader` with the given config and delegates to it. |
| `IcebergLoader` | `core/loader.py` | Orchestrator. Resolves the effective config and table properties, buffers batches up to `commit_interval`, passes each buffer to `BatchConformer`, then writes it with a `WriteStrategy`. |
| `BatchConformer` | `core/conform.py` | One instance per load. Owns the target table: loads or creates it, appends the load timestamp column, evolves the schema, and casts each buffer to the table schema. |
| `SchemaManager` | `core/schema.py` | Table lifecycle: `ensure_table_exists()` (create with partition spec and properties, or load and check the format version), `evolve_schema_if_needed()` (add top-level columns), Arrow↔Iceberg schema conversion. |
| `WriteStrategy` | `core/strategies.py` | `AppendStrategy`, `OverwriteStrategy` (overwrite on the first commit, append after), `IdempotentStrategy` (delete by `replace_filter` in the first commit, then append), `UpsertStrategy` (PyIceberg `table.upsert`). `get_write_strategy()` picks one. |
| `LoaderConfig` | `core/config.py` | Frozen Pydantic model with all load options. Validates partition expressions and rejects unsafe combinations. `TABLE_PROPERTIES` holds the default table properties. |
| `register_parquet_files` | `core/register.py` | Validates existing Parquet footers (schema, field IDs, partition statistics, paths) and references the files with `Table.add_files()` in one snapshot. |
| Partitioning | `core/partitioning.py` | Parses `day(ts)`, `bucket(16, id)`, etc. into PyIceberg transforms. |
| Arrow utilities | `utils/arrow.py` | `create_arrow_table_from_data` / `create_record_batches_from_dicts` (dicts → Arrow, nested values → JSON strings) and `convert_table_types` (cast to a target schema; a column whose safe cast fails becomes all NULL, with a warning). |
| Type mappings | `utils/types.py` | `TypeRegistry`: Arrow↔Iceberg type mapping, extensible with `register_custom_mapping()`. |
| `get_rest_catalog` | `catalog.py` | Builds a PyIceberg `RestCatalog` from arguments or environment variables. |
| Logging | `services/logging.py` | Module-level `logger` proxy over the `iceberg_loader` logger; `configure_logging()` sets level and TEXT/JSON format. |
| Maintenance | `services/maintenance.py` | `SnapshotMaintenance` and `expire_snapshots()`. |
| Compatibility shim | `iceberg_loader/iceberg_loader.py` | Keeps `from iceberg_loader.iceberg_loader import ...` working: aliases `IcebergLoader` and repeats the three wrapper functions. |

## Data flow

```
load_data_to_iceberg(pa.Table)            ──► IcebergLoader.load_data()        ── table.to_batches() ──┐
load_batches_to_iceberg(Iterator[Batch])  ──► IcebergLoader.load_data_batches()                        ├─► load_data_batches()
load_ipc_stream_to_iceberg(path/stream)   ──► IcebergLoader.load_ipc_stream()  ── pa.ipc.open_stream() ┘
register_parquet_files(paths)             ──► core/register.py ── footer schema check ── Table.add_files()

load_data_batches()
  ├─ resolve config (per-call config replaces default_config) and table properties
  ├─ strategy = get_write_strategy(write_mode, replace_filter, join_cols)
  ├─ conformer = BatchConformer(...)
  └─ for buffer in _buffered(batches, max(1, commit_interval)):     # one commit per buffer
        data = conformer.conform(buffer)
        strategy.write(conformer.table, data, is_first_write)
```

`BatchConformer.conform(buffer)`:

```
buffer of RecordBatches
  ├─ add load timestamp column (if load_timestamp is set)
  ├─ without schema_evolution: concat the buffer (mixed schemas → pa.ArrowInvalid, before any table exists)
  ├─ first call only: SchemaManager.ensure_table_exists()
  │     new table: promote string partition source to timestamp(us) for time transforms,
  │                build partition spec, apply table properties
  │     existing table: properties and partition spec are left as they are
  ├─ evolve: add the load timestamp column; with schema_evolution, add new columns from every distinct batch schema
  └─ convert_table_types() to the table's Arrow schema
        unknown columns dropped, missing columns NULL, a column with a failed safe cast all NULL + warning,
        timestamp precision loss (e.g. ns → us) warned
```

## Dependencies between modules

```
__init__ ──► core, catalog, services.logging, services.maintenance
iceberg_loader.py (shim) ──► core.loader, core.config

core.loader     ──► core.config, core.conform, core.schema, core.strategies
core.register   ──► utils.types, services.logging
core.conform    ──► core.config, core.schema, utils.arrow, services.logging
core.schema     ──► core.partitioning, utils.types, services.logging
core.config     ──► core.partitioning
core.strategies ──► services.logging

utils.arrow     ──► services.logging
utils.types     ──► (no internal imports)
core.partitioning ──► (no internal imports)
catalog         ──► (no internal imports)
services.maintenance ──► services.logging
services.logging     ──► (standard library only)
```

Third-party: `pyarrow` (`core.loader`, `core.conform`, `core.schema`, `core.strategies`, `core.register`, `utils.arrow`, `utils.types`), `pyiceberg` (`core.loader`, `core.schema`, `core.partitioning`, `core.register`, `utils.types`, `catalog`, `services.maintenance`), `pydantic` (`core.config`). Minimum versions are in `pyproject.toml`.

## Where to make changes

| Task | Go to |
|------|-------|
| Public API entry | `src/iceberg_loader/__init__.py` (and the shim if the wrapper functions change) |
| Register existing Parquet files | `core/register.py` → `register_parquet_files()` |
| Add a write mode | `core/strategies.py` → new `WriteStrategy` + `get_write_strategy()`; `core/config.py` → `write_mode` literal |
| Add or validate a config option | `core/config.py` → `LoaderConfig` |
| Change default table properties | `core/config.py` → `TABLE_PROPERTIES` |
| Change buffering / commit logic | `core/loader.py` → `IcebergLoader.load_data_batches()`, `_buffered()` |
| Change table creation, evolution, casting | `core/conform.py` → `BatchConformer`; `core/schema.py` → `SchemaManager` |
| Add a partition transform | `core/partitioning.py` |
| Add a type mapping | `utils/types.py` → `TypeRegistry` |
| Change dict → Arrow conversion | `utils/arrow.py` |
| Add a log format | `services/logging.py` → `JsonFormatter` / `TextFormatter` |
| Add a maintenance utility | `services/maintenance.py` → `SnapshotMaintenance` |
| Configure the REST catalog helper | `catalog.py` → `get_rest_catalog()` |
| Add an example | `examples/<name>.py` + `examples/README.md` + `docs/examples.md` |

## Tests

Tests run against a real sqlite-backed `SqlCatalog` (`sql_catalog` fixture in `tests/conftest.py`).

| Module | Test file |
|--------|-----------|
| `core.loader` | `tests/test_load_behavior.py`, `tests/test_iceberg_loader.py` |
| `core.register` | `tests/test_register.py` |
| `core.conform` | `tests/test_conform.py` |
| `core.config` | `tests/test_config_validation.py` |
| `core.partitioning` | `tests/test_partitioning.py` |
| `utils.arrow` | `tests/test_arrow_utils.py` |
| `utils.types` | `tests/test_type_mappings.py` |
| `services.maintenance` | `tests/test_maintenance.py` |
| `catalog` | `tests/test_catalog.py` |
| Streaming / IPC | `tests/test_streaming.py` |
| Examples smoke | `tests/test_examples_smoke.py` |
