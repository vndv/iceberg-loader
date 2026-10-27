# iceberg-loader

`iceberg-loader` wraps [PyIceberg](https://py.iceberg.apache.org/) to load PyArrow data into Apache Iceberg tables. It handles mixed JSON fields, schema evolution, partition replacement, upserts, batches, and IPC streams.

> **Status:** Actively developed and under testing. PRs are welcome!
> Tested against Hive Metastore and REST Catalog (Tabular, Polaris, self-hosted).

## Features

- **Arrow-first:** `pa.Table`, `RecordBatch`, IPC.
- **Messy JSON friendly:** dict/list/mixed → JSON strings.
- **Schema evolution** (opt-in).
- **Idempotent replace** (`replace_filter`) and **upsert**.
- **Commit interval** for long streams.
- **Maintenance helpers** (expire snapshots).

## Benchmark

[iceberg-loader-benchmark](https://github.com/vndv/iceberg-loader-benchmark) compares iceberg-loader with the [dlt](https://github.com/dlt-hub/dlt) Iceberg destination. Both write the same Arrow data through a Polaris REST Catalog and MinIO. On a 1 GiB NYC 311 snapshot (1.72M rows), iceberg-loader had a median write time of 3.7 s and a median peak RSS of 1.36 GiB; dlt took 11.4 s and 2.26 GiB. The repo covers the methodology, raw data, and how to reproduce the run.

## Install

```bash
pip install "iceberg-loader[all]"
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv add "iceberg-loader[all]"
```

### Extras

| Extra | Description |
|-------|-------------|
| `hive` | Hive Metastore support |
| `s3` | S3 filesystem support |
| `rest` | REST Catalog support (Tabular, Polaris, self-hosted) |
| `all` | All extras |

## Compatibility

- Python: 3.10, 3.11, 3.12, 3.13, 3.14
- PyArrow: >= 18.0.0
- PyIceberg: >= 0.10.0

---

## Quickstart

```python
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from iceberg_loader import LoaderConfig, load_data_to_iceberg

catalog = load_catalog("default")
data = pa.Table.from_pydict({"id": [1, 2], "signup_date": ["2023-01-01", "2023-01-02"]})

config = LoaderConfig(write_mode="append", partition_col="day(signup_date)", schema_evolution=True)
load_data_to_iceberg(data, ("db", "users"), catalog, config=config)
```

> **The default `write_mode` is `'overwrite'`.** Calling a loader without `config`, or with a `LoaderConfig` that does not set `write_mode`, replaces all data in the table. Set `write_mode="append"` explicitly unless you mean to overwrite.

### Which function to use?

| Function | Use when... | Input format |
|----------|-------------|--------------|
| `load_data_to_iceberg` | You have a single `pa.Table` in memory. | `pyarrow.Table` |
| `load_batches_to_iceberg` | You have a generator/iterator of batches (memory efficient). | Iterator of `pyarrow.RecordBatch` |
| `load_ipc_stream_to_iceberg` | You are reading from an Arrow IPC stream file/socket. | File-like object or path |

---

## Usage

### REST Catalog Setup

You can use REST Catalog via the built-in `get_rest_catalog()` helper, the `RestCatalog` constructor directly, or through a `~/.pyiceberg.yaml` configuration file.

**Option A: get_rest_catalog() helper (recommended)**

```python
from iceberg_loader import get_rest_catalog

catalog = get_rest_catalog(
    uri="https://api.tabular.io/ws",
    warehouse="s3://my-bucket/warehouse/",
    credential="your-oauth2-credential",
    s3_endpoint="https://s3.amazonaws.com",
    s3_region="us-east-1",
)
```

The helper also reads from environment variables — see `help(get_rest_catalog)` for all options.

**Option B: Direct constructor**

```python
import os
from pyiceberg.catalog.rest import RestCatalog

# Only include S3 properties that are actually set, so unset env vars
# don't pass None into RestCatalog.
s3_properties = {
    key: value
    for key, value in {
        "s3.endpoint": os.environ.get("S3_ENDPOINT"),
        "s3.access-key-id": os.environ.get("S3_ACCESS_KEY"),
        "s3.secret-access-key": os.environ.get("S3_SECRET_KEY"),
        "s3.region": os.environ.get("S3_REGION"),
    }.items()
    if value is not None
}

catalog = RestCatalog(
    name="my-rest-catalog",
    uri=os.environ["ICEBERG_REST_URI"],          # e.g. https://api.tabular.io/ws
    warehouse=os.environ["ICEBERG_WAREHOUSE"],   # e.g. s3://my-bucket/warehouse/
    credential=os.environ["ICEBERG_CREDENTIAL"], # OAuth2 credential
    **s3_properties,
)
```

> Prefer `get_rest_catalog()` (Option A) — it handles env-var resolution and
> omits unset properties for you.

**Option C: pyiceberg.yaml + load_catalog()**

```yaml
# ~/.pyiceberg.yaml (see examples/pyiceberg.yaml.sample for a full annotated example)
catalog:
  my-rest-catalog:
    type: rest
    uri: https://api.tabular.io/ws
    warehouse: s3://my-bucket/warehouse/
    credential: your-oauth2-credential
    s3.endpoint: https://s3.amazonaws.com
    s3.region: us-east-1
```

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("my-rest-catalog")
```

### Basic Example

```python
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from iceberg_loader import LoaderConfig, load_data_to_iceberg

catalog = load_catalog("default")

data = pa.Table.from_pydict({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "created_at": [1672531200000, 1672617600000, 1672704000000],
    "signup_date": ["2023-01-01", "2023-01-01", "2023-01-02"]
})

config = LoaderConfig(write_mode="append", partition_col="day(signup_date)", schema_evolution=True)
result = load_data_to_iceberg(
    table_data=data,
    table_identifier=("my_db", "my_table"),
    catalog=catalog,
    config=config,
)

print(result)
# {'rows_loaded': 3, 'write_mode': 'append', 'partition_col': 'day(signup_date)', ...}
```

### Preparing Data

These helpers convert Python dictionaries to Arrow data, including mixed fields:

```python
from iceberg_loader.utils.arrow import create_arrow_table_from_data, create_record_batches_from_dicts

# 1. Convert list of dicts -> pa.Table
arrow_table = create_arrow_table_from_data(data_list)

# 2. Convert iterator of dicts -> Iterator[pa.RecordBatch]
batches = create_record_batches_from_dicts(data_generator(), batch_size=10000)
```

Alternatively, use standard PyArrow conversion: `pa.Table.from_pylist(data)`.

The helpers serialize dicts and lists as JSON and convert other non-null values to strings too. For example, an integer `id` becomes an Arrow string. If you need to preserve scalar types, prepare a typed Arrow table with PyArrow instead.

The helpers sort columns by name, so a table they create gets alphabetical column order. Existing tables keep their own order.

### Idempotent Load (Replace Partition)

Replace data for a specific day without adding duplicate rows:

```python
day_data = pa.Table.from_pydict({
    "id": [1, 2],
    "name": ["Alice", "Bob"],
    "signup_date": ["2023-01-01", "2023-01-01"],
})

config = LoaderConfig(
    write_mode="append",
    replace_filter="signup_date == '2023-01-01T00:00:00'",
    partition_col="day(signup_date)",
)

load_data_to_iceberg(table_data=day_data, table_identifier=("my_db", "my_table"), catalog=catalog, config=config)
```

The filter must cover every row you load: rows outside it are not deleted first, so they are duplicated on each rerun.

A string column partitioned with `year`/`month`/`day`/`hour` is stored as `timestamp(us)` when the loader creates the table, so the filter must compare it with a full timestamp literal (`'2023-01-01T00:00:00'`). A date-only literal such as `'2023-01-01'` raises `ValueError: Invalid timestamp without zone`. A column that is already `date` typed keeps its type, so filter it with a date literal (`'2023-01-01'`).

The delete and the first commit run in one transaction; later commits of the same load are separate (see [Commits and atomicity](#commits-and-atomicity)).

### Upsert (Merge Into)

Update matching rows and insert new ones using key columns. `join_cols` is required: tables created by the loader have no identifier fields, so PyIceberg cannot infer the keys.

```python
config = LoaderConfig(write_mode="upsert", join_cols=["id"])
load_data_to_iceberg(table_data=data, table_identifier=("my_db", "my_table"), catalog=catalog, config=config)
```

### Batch Loading

For large datasets, use `load_batches_to_iceberg` with an iterator of RecordBatches:

```python
from iceberg_loader import load_batches_to_iceberg

def batch_generator():
    for i in range(10):
        yield some_record_batch

result = load_batches_to_iceberg(
    batch_iterator=batch_generator(),
    table_identifier=("my_db", "large_table"),
    catalog=catalog,
    config=LoaderConfig(write_mode="append", commit_interval=100),
)
```

### Stream Loading (Arrow IPC)

Load data directly from an Apache Arrow IPC stream:

```python
from iceberg_loader import load_ipc_stream_to_iceberg

result = load_ipc_stream_to_iceberg(
    stream_source="data.arrow",
    table_identifier=("my_db", "stream_table"),
    catalog=catalog,
    config=LoaderConfig(write_mode="append"),
)
```

### Custom Settings

Set table properties for a new table:

```python
custom_props = {
    'write.parquet.compression-codec': 'snappy',
    'history.expire.min-snapshots-to-keep': 5,
}

config = LoaderConfig(table_properties=custom_props, write_mode="append")
load_data_to_iceberg(..., config=config)
```

`table_properties` apply **only when the loader creates the table**. Loading into an existing table never changes any of its properties: with the example above, a table created earlier with `zstd` keeps `zstd`. To change properties of an existing table, use PyIceberg directly (`table.transaction().set_properties(...)`).

### Override Default Table Properties Globally

Built-in defaults live in `iceberg_loader.core.config.TABLE_PROPERTIES` (format version, Parquet compression, commit retries). To tweak them, copy the dictionary, override keys, and pass into `LoaderConfig`:

```python
from pyiceberg.catalog import load_catalog
from iceberg_loader import LoaderConfig, load_data_to_iceberg
from iceberg_loader.core.config import TABLE_PROPERTIES

catalog = load_catalog("default")

custom_properties = {**TABLE_PROPERTIES, "write.parquet.compression-codec": "gzip"}

config = LoaderConfig(
    write_mode="append",
    table_properties=custom_properties,
)

load_data_to_iceberg(table_data, ("default", "events"), catalog, config=config)
```

Because this copies `format-version` into your own `table_properties`, it counts as set explicitly: loading into an existing table of another version logs a warning (see below).

### Timestamp precision and format version

- Iceberg format v1/v2 stores timestamps in microseconds. Nanosecond input (`pa.timestamp('ns')`) is truncated to microseconds; when values actually lose precision, the loader logs a warning naming the column.
- `format-version` in `table_properties` applies only when the loader creates a table. Loading into an existing table never changes its version; if you set `format-version` explicitly and it differs from the table's version, the loader logs a warning. The library default (`format-version: 2`) does not trigger it.
- PyIceberg cannot write format v3 tables yet. Requesting `format-version: 3` for a new table, or loading into an existing v3 table, raises `NotImplementedError` before anything is written.
- A `format-version` that is not a whole number (e.g. `'v2'` or `2.9`) is ignored for existing tables, with a warning.

For timestamp columns, prefer partition transforms such as `day(ts)` or `hour(ts)`, especially when using `load_timestamp`.

### Maintenance Helper

```python
from iceberg_loader import expire_snapshots

table = catalog.load_table(("db", "users"))
expire_snapshots(table, keep_last=2)
```

- `keep_last` (default `1`) is meant to keep the N most recent snapshots; a negative value skips expiration.
- `older_than_ms` (epoch milliseconds) expires snapshots older than that time instead; when set, `keep_last` is ignored.
- **Known issue:** the cutoff is computed as a local naive `datetime`, which PyIceberg reads as UTC. On a host whose timezone is not UTC, both options are shifted by the UTC offset, so more or fewer snapshots than requested can be expired. `keep_last=0` expires nothing. Run maintenance with `TZ=UTC` until this is fixed.
- Errors during expiration (commit conflicts, I/O, invalid arguments) are logged as warnings and not raised.

### Adding Load Timestamp

Set `load_timestamp` to add a timestamp column (by default, `_load_dttm`) to every row. You can use it for audit trails or partitioning by load time.

```python
from datetime import datetime

# Will add column '_load_dttm' with current time and partition by hour
config = LoaderConfig(
    write_mode="append",
    load_timestamp=datetime.now(),
    partition_col="hour(_load_dttm)",
)

# You can also customize the column name and day-transform it
config_custom = LoaderConfig(
    write_mode="append",
    load_timestamp=datetime(2025, 1, 1),
    load_ts_col="etl_ts",
    partition_col="day(etl_ts)",
)
```

### Reusing a Loader (`IcebergLoader`)

The `load_*_to_iceberg` functions create a new `IcebergLoader` on every call. To share settings across calls, create one yourself:

```python
from iceberg_loader import IcebergLoader, LoaderConfig

loader = IcebergLoader(
    catalog,
    table_properties={"write.parquet.compression-codec": "snappy"},
    default_config=LoaderConfig(write_mode="append", schema_evolution=True),
)

loader.load_data(arrow_table, ("db", "users"))                     # uses default_config
loader.load_data_batches(batches, ("db", "events"))                 # uses default_config
loader.load_ipc_stream("data.arrow", ("db", "stream_table"))
loader.load_data(arrow_table, ("db", "users"), config=LoaderConfig(write_mode="overwrite"))
```

- A per-call `config` **replaces** `default_config` entirely; fields are not merged. In the last call above, `schema_evolution` is back to `False`.
- Table properties for a new table are merged in this order, later wins: `TABLE_PROPERTIES` → `IcebergLoader(table_properties=...)` → `LoaderConfig.table_properties`.

### Logging

The library logs to the `iceberg_loader` logger. On first use it sets that logger to `INFO`, attaches its own `stderr` handler, and sets `propagate=False`, so messages do not reach your root logger handlers. To change the level or switch to JSON output, call `configure_logging` before loading:

```python
from iceberg_loader.services.logging import configure_logging

configure_logging(level="WARNING", log_format="JSON")
```

To route messages through your own handlers instead, configure first, then adjust the logger it returns (for example, clear its handlers and set `propagate = True`).

---

## Behavior & gotchas

### Commits and atomicity

- Each commit covers up to `commit_interval` batches. `commit_interval=0` (the default) and `1` both mean **one commit per batch**, not one transaction for the whole load.
- `load_data_to_iceberg` splits a `pa.Table` into its record batches (one per chunk), so a multi-chunk table is also written in several commits.
- A load that fails midway leaves the commits that already succeeded. With `replace_filter`, the delete happens in the first commit only, so a failed load can leave the old rows deleted and the new rows partially written. With `write_mode="overwrite"`, the table is overwritten by the first commit and appended to by the rest.
- To write a load in a single commit, set `commit_interval` to at least the number of batches (for a `pa.Table`, `len(table.to_batches())`, or call `table.combine_chunks()` first to get a single batch).

### Write modes

- The default `write_mode` is `'overwrite'`. See the note in [Quickstart](#quickstart).
- `replace_filter` works with `'append'` and `'overwrite'`: both delete matching rows and append (delete-then-append), so `'overwrite'` with a filter does not overwrite the whole table.
- `replace_filter` must match every row being loaded; rows outside the filter are appended again on each rerun.
- `replace_filter` cannot be combined with `write_mode='upsert'` (rejected by `LoaderConfig`).
- Upsert needs `join_cols` (see [Upsert](#upsert-merge-into)).

### Schema and types

- `schema_evolution=False` (the default): columns that are not in the table are **dropped silently**. Columns missing from the data are written as NULL.
- With `schema_evolution=False` and `commit_interval > 1`, all batches in one commit must have the same schema, including column order and `string` vs `large_string`; otherwise the load fails with `pyarrow.lib.ArrowInvalid`. Enable `schema_evolution` or keep batch schemas identical.
- `schema_evolution=True` adds new top-level columns only. It does not change existing column types.
- Data is cast to the table's column types. If a safe cast fails for **any** value in a column, the **whole column of that commit** is written as NULL, and a warning naming the column is logged.
- Lossy casts that PyArrow can perform unsafely go through **without a warning**: floats are truncated to integers (`1.9 → 1`), and integer overflow wraps (`int64 → int32`). The only lossy cast that warns is timestamp precision loss (e.g. ns → us).
- Nested Arrow types (`struct`, `list`, `map`) are not supported: creating or evolving a table with them raises `ValueError: Unsupported PyArrow type`. Use `create_arrow_table_from_data` to serialize nested values as JSON strings.

### Partitioning

- `partition_col` accepts one field: a column name or a transform (`year`, `month`, `day`, `hour`, `bucket(N, col)`, `truncate(N, col)`).
- The partition spec is set only when the loader creates the table. Changing `partition_col` later does not repartition an existing table.
- A string column used with `year`/`month`/`day`/`hour` is promoted to `timestamp(us)` at table creation; filter it with timestamp literals (see [Idempotent Load](#idempotent-load-replace-partition)).
- If `partition_col` names a column that is not in the data, the table is created **without partitions** and only a warning is logged.
- A transform that does not fit the column type (for example, `day(...)` on an integer column) fails the load with a `ValidationError`.
- Identity partitioning on the load timestamp column is rejected; use `day(...)` or `hour(...)`.

### Table properties

- All `table_properties`, including `format-version`, apply only when the loader creates the table (see [Custom Settings](#custom-settings)).

---

## LoaderConfig Reference

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `write_mode` | `'append'` \| `'overwrite'` \| `'upsert'` | `'overwrite'` | Data write mode. **The default overwrites the table**; set `'append'` explicitly to add data |
| `partition_col` | `str \| None` | `None` | Partition column or transform (e.g., `month(ts)`, `bucket(16,id)`); one field, applied only when the table is created; prefer day/hour on timestamps |
| `replace_filter` | `str \| None` | `None` | SQL-style filter for idempotent loads (delete-then-append) |
| `schema_evolution` | `bool` | `False` | Auto-add new columns; when `False`, unknown columns are dropped |
| `commit_interval` | `int` | `0` | Commit every N batches; `0` and `1` both commit after every batch |
| `join_cols` | `list[str] \| None` | `None` | Merge keys for upsert (required for `'upsert'`) |
| `table_properties` | `dict \| None` | `None` | Custom Iceberg table properties; applied only when the loader creates the table |
| `load_timestamp` | `datetime \| None` | `None` | If set, adds a `load_ts_col` column (default `_load_dttm`) with this value |
| `load_ts_col` | `str` | `'_load_dttm'` | Name of the load timestamp column |

---

## API Reference

Pass write options **only** through `LoaderConfig`; the table above lists its fields.

### `load_data_to_iceberg()`

Loads a `pa.Table` held in memory. The table is written batch by batch (one batch per chunk).

```python
def load_data_to_iceberg(
    table_data: pa.Table,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    config: LoaderConfig | None = None,
) -> dict[str, Any]
```

### `load_batches_to_iceberg()`

Loads a stream of batches into an Iceberg table.

```python
def load_batches_to_iceberg(
    batch_iterator: Iterator[pa.RecordBatch] | pa.RecordBatchReader,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    config: LoaderConfig | None = None,
) -> dict[str, Any]
```

### `load_ipc_stream_to_iceberg()`

Reads an Arrow IPC stream (path, file-like object, or `pa.NativeFile`) and loads its batches.

```python
def load_ipc_stream_to_iceberg(
    stream_source: str | BinaryIO | pa.NativeFile,
    table_identifier: tuple[str, str],
    catalog: Catalog,
    config: LoaderConfig | None = None,
) -> dict[str, Any]
```

### `IcebergLoader`

```python
class IcebergLoader:
    def __init__(
        self,
        catalog: Catalog,
        table_properties: dict[str, Any] | None = None,
        default_config: LoaderConfig | None = None,
    ): ...

    def load_data(self, table_data, table_identifier, config=None) -> dict[str, Any]: ...
    def load_data_batches(self, batch_iterator, table_identifier, config=None) -> dict[str, Any]: ...
    def load_ipc_stream(self, stream_source, table_identifier, config=None) -> dict[str, Any]: ...
```

See [Reusing a Loader](#reusing-a-loader-icebergloader) for how `default_config` and `table_properties` combine.

### `get_rest_catalog()`

```python
def get_rest_catalog(
    name: str = "rest-catalog",
    *,
    uri: str | None = None,              # env: ICEBERG_REST_URI (required)
    warehouse: str | None = None,        # env: ICEBERG_WAREHOUSE
    credential: str | None = None,       # env: ICEBERG_CREDENTIAL
    s3_endpoint: str | None = None,      # env: S3_ENDPOINT
    s3_access_key: str | None = None,    # env: S3_ACCESS_KEY
    s3_secret_key: str | None = None,    # env: S3_SECRET_KEY
    s3_region: str | None = None,        # env: S3_REGION
    s3_path_style: bool = False,
    extra_properties: dict[str, str] | None = None,
) -> RestCatalog
```

Explicit arguments win over environment variables. Path-style S3 access is enabled automatically when an S3 endpoint is set. `extra_properties` are applied last and override every other key. Raises `ValueError` when no URI is given.

### `expire_snapshots()`

```python
def expire_snapshots(table: Table, keep_last: int = 1, older_than_ms: int | None = None) -> None
```

See [Maintenance Helper](#maintenance-helper).

### Return value of the load functions

```python
{
    'rows_loaded': int,
    'batches_processed': int,
    'write_mode': str,
    'partition_col': str,        # the string 'none' when not set
    'table_location': str,       # the string 'none' when no batches were loaded
    'snapshot_id': int | str,    # the string 'none' when there is no snapshot
    'new_table_created': bool,
    'conform_ms': float,         # conforming batches: table load/create, schema evolution commits, casts
    'write_ms': float,           # writing and committing batches
    'total_ms': float,           # the whole load, excluding config resolution
    'casts_applied': int,        # columns successfully cast (including lossy)
    'casts_lossy': int,          # subset of applied that only succeeded in unsafe mode (truncated or wrapped values)
    'casts_failed': int,         # columns with a failed cast, filled with NULLs
}
```

Note that the "empty" values are the string `'none'`, not `None`.

- **Timings:** `total_ms - conform_ms - write_ms` is mostly the time spent reading the input (a generator, an IPC stream) plus the final snapshot lookup, so a slow source shows up there, not in `write_ms`.
- **Cast counters** count distinct columns per load, not rows, batches, or buffers: a column cast in every batch counts once, regardless of `commit_interval`, `schema_evolution`, or how the input is chunked. `casts_lossy` covers timestamp precision loss, float→int truncation, and integer overflow (wrapped values). A column whose batches behaved differently can be counted in both `casts_applied` and `casts_failed`.

Each successful load emits one `METRIC: load_completed` info log with the same timing and cast fields. A load that raises emits no metric.

### Usage Examples

**Basic stream loading:**
```python
def generate_batches():
    for i in range(100):
        data = {"id": [i], "value": [f"row_{i}"]}
        yield pa.RecordBatch.from_pydict(data)

result = load_batches_to_iceberg(
    batch_iterator=generate_batches(),
    table_identifier=("db", "table"),
    catalog=catalog,
    config=LoaderConfig(write_mode="append")
)
```

**With commits for memory management:**
```python
result = load_batches_to_iceberg(
    batch_iterator=large_batch_stream,
    table_identifier=("db", "large_table"),
    catalog=catalog,
    config=LoaderConfig(
        write_mode="append",
        commit_interval=50,
        schema_evolution=True
    )
)
```

**Idempotent partition loading:**
```python
result = load_batches_to_iceberg(
    batch_iterator=daily_batches,  # event_date is a string column, e.g. "2023-12-09"
    table_identifier=("db", "events"),
    catalog=catalog,
    config=LoaderConfig(
        write_mode="append",
        replace_filter="event_date == '2023-12-09T00:00:00'",
        partition_col="day(event_date)"
    )
)
```

---

## Examples

See [Examples](examples.md) for scripts covering streaming, upserts, schema evolution, and mixed JSON fields.

For how the package is put together, see [Architecture](ARCHITECTURE.md).

---

## Public API & versioning

- Semantic Versioning from `0.1.x`: MINOR = new compatible features, PATCH = fixes, MAJOR = breaking changes. Breaking changes to documented APIs are noted in `RELEASE.md`.
- Public API: `LoaderConfig`, `IcebergLoader`, `load_data_to_iceberg`, `load_batches_to_iceberg`, `load_ipc_stream_to_iceberg`, `get_rest_catalog`, `expire_snapshots`. Data conversion helpers live in `iceberg_loader.utils.arrow`. Other modules are internal.
- Pass loading options via `LoaderConfig`; avoid legacy positional arguments.
- Prefer partition transforms for timestamps (`day(ts)`, `hour(ts)`, `day(_load_dttm)`) to avoid unbounded partition counts.
- LoaderConfig validates partition expressions and forbids unsafe mixes (e.g., `replace_filter` with `upsert`, identity partition on `_load_dttm`).

---

## Development

See [CONTRIBUTING.md](https://github.com/vndv/iceberg-loader/blob/main/CONTRIBUTING.md) for dev setup, tests, code style, and the release checklist.

---

## License

`iceberg-loader` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
