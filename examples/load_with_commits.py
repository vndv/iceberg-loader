import logging
import sys
import time
from pathlib import Path

import pyarrow as pa

# Ensure parent directory (examples/) is on path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from catalog import get_catalog

from iceberg_loader import LoaderConfig, load_batches_to_iceberg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def generate_batches(num_batches=20, batch_size=10):
    """Generator that yields RecordBatches."""
    for i in range(num_batches):
        data = {
            'id': range(i * batch_size, (i + 1) * batch_size),
            'batch_id': [i] * batch_size,
            'ts': [time.time()] * batch_size,
        }
        batch = pa.RecordBatch.from_pydict(data)
        yield batch


def run_example():
    catalog = get_catalog()
    table_id = ('default', 'commit_interval_test')

    # Cleanup
    try:
        catalog.drop_table(table_id)
        logger.info('Dropped old table %s', table_id)
    except Exception:
        pass

    logger.info('Starting load with commit_interval=5...')

    # We will load 20 batches, committing every 5 batches.
    # This means we expect roughly 4 snapshots (transactions) to be created.

    config = LoaderConfig(write_mode='append', commit_interval=5)

    result = load_batches_to_iceberg(
        batch_iterator=generate_batches(num_batches=20, batch_size=100),
        table_identifier=table_id,
        catalog=catalog,
        config=config,
    )

    logger.info('Load complete. Result: %s', result)

    # Verify
    table = catalog.load_table(table_id)
    snapshots = list(table.snapshots())
    logger.info('Table has %d snapshots (commits)', len(snapshots))

    # We expect 4 snapshots if 20 batches / 5 interval = 4 commits.
    # (Assuming no other operations interfered)
    for i, snap in enumerate(snapshots):
        logger.info('Snapshot %d: ID=%s, Timestamp=%s', i + 1, snap.snapshot_id, snap.timestamp_ms)

    # Read back count
    total_rows = len(table.scan().to_arrow())
    logger.info('Total rows in table: %d', total_rows)
    assert total_rows == 2000, f'Expected 2000 rows, got {total_rows}'


if __name__ == '__main__':
    run_example()
