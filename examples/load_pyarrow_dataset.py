import logging
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from catalog import get_catalog
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError

from iceberg_loader import LoaderConfig, load_batches_to_iceberg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ROWS_PER_FILE = 10


def write_source_parquet(directory: Path) -> int:
    """Write one Parquet file per day and return the total number of rows."""
    total_rows = 0
    for day in range(3):
        day_table = pa.table(
            {
                'id': list(range(day * ROWS_PER_FILE, (day + 1) * ROWS_PER_FILE)),
                'day': [day] * ROWS_PER_FILE,
                'value': [float(day * 100 + i) for i in range(ROWS_PER_FILE)],
            }
        )
        pq.write_table(day_table, directory / f'part-{day}.parquet')
        total_rows += day_table.num_rows
    return total_rows


def count_snapshots(catalog: Catalog, table_id: tuple[str, str]) -> int:
    try:
        return len(catalog.load_table(table_id).snapshots())
    except NoSuchTableError:
        return 0


def main() -> None:
    catalog = get_catalog()
    table_id = ('default', 'pyarrow_dataset_example')

    with tempfile.TemporaryDirectory(prefix='iceberg-loader-dataset-') as tmp:
        source_dir = Path(tmp)
        expected_rows = write_source_parquet(source_dir)

        snapshots_before = count_snapshots(catalog, table_id)
        source = ds.dataset(source_dir, format='parquet')
        logger.info('Streaming %d rows from %s', source.count_rows(), source_dir)

        # to_batches() returns a RecordBatchReader, which load_batches_to_iceberg consumes
        # directly. commit_interval=2 turns the stream into multiple commits.
        result = load_batches_to_iceberg(
            batch_iterator=source.to_batches(batch_size=ROWS_PER_FILE),
            table_identifier=table_id,
            catalog=catalog,
            config=LoaderConfig(write_mode='overwrite', commit_interval=2),
        )

    logger.info('Loaded %s: %s', table_id, result)

    table = catalog.load_table(table_id)
    arrow = table.scan().to_arrow()
    assert arrow.num_rows == expected_rows, f'Expected {expected_rows} rows, got {arrow.num_rows}'
    assert result['rows_loaded'] == expected_rows, (
        f'Expected result rows_loaded={expected_rows}, got {result["rows_loaded"]}'
    )
    assert result['batches_processed'] == 3, f'Expected 3 batches, got {result["batches_processed"]}'
    assert max(arrow.column('id').to_pylist()) == expected_rows - 1, 'Unexpected max id after read-back'
    assert arrow.column('day').to_pylist().count(2) == ROWS_PER_FILE, 'Expected a full day=2 partition'
    new_snapshots = len(table.snapshots()) - snapshots_before
    assert new_snapshots >= 2, f'commit_interval=2 should produce at least two commits, got {new_snapshots}'
    logger.info('Read-back verified: %d rows across %d batches', arrow.num_rows, result['batches_processed'])


if __name__ == '__main__':
    main()
