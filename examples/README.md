# Examples

This directory contains runnable examples demonstrating various features of `iceberg-loader`.

## Prerequisites

You need a running Iceberg catalog (e.g., REST catalog) and MinIO/S3.
A `docker-compose.yml` is provided to spin up a local environment.

```bash
cd examples
docker-compose up -d
```

Trino connection:

```
host: localhost
port: 8080
database: iceberg
username: trion
password: <empty>
```

MinIO:

```
endpoint: http://localhost:9001
access_key: minio
secret_key: minio123
```

## Install dependencies with UV

```bash
uv init --python3.14
uv add "iceberg-loader[all]" 
```

## Running Examples

Run from the `examples/` directory with `uv`:

```bash
# Upsert
uv run python load_upsert.py

# Commit interval for long streams
uv run python load_with_commits.py

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
```

## Example summary

- `load_upsert.py`: upsert by keys.
- `load_with_commits.py`: commit_interval for streams.
- `compare_complex_json_fail.py`: PyArrow fails on mixed types, `iceberg-loader` succeeds.
- `advanced_scenarios.py`: schema evolution, custom types, partitioning.
- `load_stream.py`: Arrow IPC stream.
- `load_from_api.py`: REST API batches.
- `maintenance_example.py`: snapshot expiration.
