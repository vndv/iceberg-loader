# Examples

This directory contains runnable examples demonstrating various features of `iceberg-loader`.

## Prerequisites

You need a running Iceberg catalog (e.g., REST catalog) and MinIO/S3.
A `docker-compose.yml` is provided to spin up a local environment.

   ```bash
   docker-compose up -d
   ```

## Running Examples

Run from the `examples/` directory with `uv` (core examples):

```bash
# Basic load
uv run python load_example.py

# Upsert
uv run python load_upsert.py

# Commit interval for long streams
uv run python load_with_commits.py

# Messy JSON: PyArrow failure vs iceberg-loader success
uv run python compare_complex_json_fail.py

# Advanced scenarios (schema evolution, types, partitioning)
uv run python advanced_scenarios.py
```

Optional (in `optional/` subfolder, if needed):

```bash
# Arrow IPC stream loading
uv run python optional/load_stream.py

# Simulated REST API loading
uv run python optional/load_from_api.py

# Maintenance: expiring snapshots
uv run python optional/maintenance_example.py
```

## Example summary

- `load_example.py`: basic load.
- `load_upsert.py`: upsert by keys.
- `load_with_commits.py`: commit_interval for streams.
- `compare_complex_json_fail.py`: PyArrow fails on mixed types, `iceberg-loader` succeeds.
- `advanced_scenarios.py`: schema evolution, custom types, partitioning.
- `optional/load_stream.py`: Arrow IPC stream.
- `optional/load_from_api.py`: REST API batches.
- `optional/maintenance_example.py`: snapshot expiration.
