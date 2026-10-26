# Examples

These examples show data loading, catalog connections, and maintenance with `iceberg-loader`.

## Prerequisites

You need a running Iceberg catalog and MinIO/S3. The bundled `docker-compose.yml`
starts Hive Metastore, MinIO, Trino, and an Apache Polaris REST Catalog server.

```bash
cd examples
docker compose up -d
```

The bundled stack includes:

- **MinIO** (S3) at `http://localhost:9000` (console `http://localhost:9001`, `minio`/`minio123`)
- **Hive Metastore** at `thrift://localhost:9083`
- **Trino** at `http://localhost:8080`
- **Apache Polaris** REST Catalog at `http://localhost:8181/api/catalog` (`root`/`root`)

`rest_catalog_example.py` uses the bundled Polaris catalog by default. For a remote
catalog such as Tabular or a separate Polaris server, set these environment variables:

```bash
export ICEBERG_REST_URI="https://your-rest-catalog-server.com/api"
export ICEBERG_WAREHOUSE="your-catalog-name"
export ICEBERG_CREDENTIAL="your-oauth2-credential"
export ICEBERG_OAUTH2_SERVER_URI="https://your-rest-catalog-server.com/api/v1/oauth/tokens"
export S3_ENDPOINT="https://your-s3-endpoint"
export S3_ACCESS_KEY="your-s3-access-key"
export S3_SECRET_KEY="your-s3-secret-key"
export S3_REGION="your-s3-region"
```

Set the S3 variables to values for the remote catalog's storage. The example defaults to local MinIO when they are unset.

Alternatively, copy `examples/pyiceberg.yaml.sample` to `~/.pyiceberg.yaml`, adjust the values,
and use `load_catalog("my-rest-catalog")` instead of the direct constructor.

> **Polaris persistence caveat:** the bundled Polaris server uses in-memory persistence,
> so the `datalake` catalog is wiped whenever the Polaris container restarts. The
> `polaris-setup` sidecar re-creates it on every `docker compose up`.

Trino connection:

```
host: localhost
port: 8080
database: iceberg
username: trino
password: <empty>
```

MinIO:

```
endpoint: http://localhost:9000
console: http://localhost:9001
access_key: minio
secret_key: minio123
```

## Install dependencies with uv

From the repository root, install the project's dependencies and run the examples against this checkout:

```bash
uv sync --all-extras
cd examples
```

## Run the examples

Run from the `examples/` directory with `uv`:

```bash
# REST Catalog example
uv run python rest_catalog_example.py

# Upsert
uv run python load_upsert.py

# Commit interval for long streams
uv run python load_with_commits.py

# Messy JSON
uv run python load_complex_json.py

# Messy JSON: PyArrow failure vs iceberg-loader success
uv run python compare_complex_json_fail.py

# Advanced scenarios (schema evolution, types, partitioning)
uv run python advanced_scenarios.py
```

Other examples:

```bash
# Arrow IPC stream loading
uv run python load_stream.py

# Simulated REST API loading
uv run python load_from_api.py

# Maintenance: expiring snapshots
uv run python maintenance_example.py

# Load timestamp column + time-transform partitioning
uv run python load_timestamp_partitioning.py
```

Run the local smoke subset:

```bash
bash ../tools/run_examples_smoke.sh
```

The smoke suite runs the fast examples against the bundled Docker stack, including
`rest_catalog_example.py`. It skips:

- `load_from_api.py`: depends on an external public API.
- `load_stream.py`: writes a very large in-memory IPC stream and is too heavy for routine smoke checks.

## Example summary

- `rest_catalog_example.py`: REST Catalog connection, table creation, append, read-back.
- `load_upsert.py`: upsert by keys.
- `load_with_commits.py`: commit_interval for streams.
- `load_complex_json.py`: dicts, lists, and mixed values serialized to JSON strings.
- `compare_complex_json_fail.py`: PyArrow fails on mixed types, `iceberg-loader` succeeds.
- `advanced_scenarios.py`: schema evolution, custom types, partitioning.
- `load_stream.py`: Arrow IPC stream.
- `load_from_api.py`: REST API batches.
- `maintenance_example.py`: snapshot expiration.
- `load_timestamp_partitioning.py`: `_load_dttm` load timestamp with `day`/`hour` partitioning, and the rejected identity partition.
