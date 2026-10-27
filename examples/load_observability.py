import logging

import pyarrow as pa
from catalog import get_catalog

from iceberg_loader import LoaderConfig, load_batches_to_iceberg, load_data_to_iceberg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TABLE_ID = ('default', 'load_observability_example')


def drifted_batches(num_batches: int = 3):
    """Batches whose types drifted from the table: float quantities and string codes."""
    for i in range(num_batches):
        yield pa.RecordBatch.from_pydict(
            {
                'id': pa.array([i * 2, i * 2 + 1], type=pa.int64()),
                # Table column is int32: 1.5 only fits with an unsafe (truncating) cast.
                'qty': pa.array([1.5, 2.0], type=pa.float64()),
                # Table column is int64: 'n/a' cannot be cast, so the column is filled with NULLs.
                'code': pa.array(['n/a', 'n/a'], type=pa.string()),
            },
        )


def main() -> None:
    catalog = get_catalog()

    # Run 1: create the table with the expected types. No casts are needed.
    initial = pa.table(
        {
            'id': pa.array([100], type=pa.int64()),
            'qty': pa.array([1], type=pa.int32()),
            'code': pa.array([7], type=pa.int64()),
        },
    )
    result = load_data_to_iceberg(initial, TABLE_ID, catalog, LoaderConfig(write_mode='overwrite'))
    logger.info(
        'Run 1 timings: conform=%sms write=%sms total=%sms',
        *(result[k] for k in ('conform_ms', 'write_ms', 'total_ms')),
    )
    assert (result['casts_applied'], result['casts_lossy'], result['casts_failed']) == (0, 0, 0), result

    # Run 2: three drifted batches, one commit per batch. Cast counters count distinct columns per load,
    # so each column is counted once even though it was cast in every batch.
    result = load_batches_to_iceberg(drifted_batches(), TABLE_ID, catalog, LoaderConfig(write_mode='append'))
    logger.info(
        'Run 2: %d rows in %d batches, casts applied=%d lossy=%d failed=%d, total=%sms',
        result['rows_loaded'],
        result['batches_processed'],
        result['casts_applied'],
        result['casts_lossy'],
        result['casts_failed'],
        result['total_ms'],
    )
    assert result['batches_processed'] == 3, result
    assert result['casts_applied'] == 1, result  # qty
    assert result['casts_lossy'] == 1, result  # qty, truncated
    assert result['casts_failed'] == 1, result  # code, NULL-filled
    assert result['conform_ms'] + result['write_ms'] <= result['total_ms'], result

    rows = catalog.load_table(TABLE_ID).scan(row_filter='id < 100').to_arrow().sort_by('id').to_pylist()
    assert [row['qty'] for row in rows] == [1, 2] * 3, rows
    assert all(row['code'] is None for row in rows), rows
    logger.info('Read-back verified: qty truncated to int, code filled with NULLs')


if __name__ == '__main__':
    main()
