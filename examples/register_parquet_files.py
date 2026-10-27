import io
import logging

import pyarrow as pa
import pyarrow.parquet as pq
from catalog import get_catalog
from pyiceberg.table import Table

from iceberg_loader import LoaderConfig, load_data_to_iceberg, register_parquet_files

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TABLE_ID = ('default', 'register_parquet_files_example')
EVENTS_SCHEMA = pa.schema([('id', pa.int64()), ('event', pa.string())])


def write_external_parquet(table: Table, path: str, rows: list[tuple[int, str]]) -> None:
    """Stand-in for an external writer (Spark, DuckDB, a vendor export) that drops Parquet files on S3."""
    data = pa.table({'id': [row[0] for row in rows], 'event': [row[1] for row in rows]}, schema=EVENTS_SCHEMA)
    buffer = io.BytesIO()
    pq.write_table(data, buffer)
    with table.io.new_output(path).create(overwrite=True) as output:
        output.write(buffer.getvalue())


def main() -> None:
    catalog = get_catalog()

    # The table must exist before files can be registered; an overwrite load keeps re-runs deterministic.
    seed = pa.table({'id': [0], 'event': ['seed']}, schema=EVENTS_SCHEMA)
    load_data_to_iceberg(seed, TABLE_ID, catalog, LoaderConfig(write_mode='overwrite'))
    table = catalog.load_table(TABLE_ID)

    # Files outside the table's data directory: they are referenced as-is, never copied or rewritten.
    landing = f'{table.location()}/landing'
    paths = [f'{landing}/events-1.parquet', f'{landing}/events-2.parquet']
    write_external_parquet(table, paths[0], [(1, 'click'), (2, 'view')])
    write_external_parquet(table, paths[1], [(3, 'click')])

    result = register_parquet_files(paths, TABLE_ID, catalog, snapshot_properties={'source': 'landing'})
    logger.info('Registered files: %s', result)
    assert result['files_added'] == 2, result
    assert result['rows_added'] == 3, result

    # Registering the same files again would duplicate rows, so it is rejected before any commit.
    try:
        register_parquet_files(paths, TABLE_ID, catalog)
    except ValueError as error:
        logger.info('Second registration rejected as expected: %s', error)
    else:
        raise AssertionError('Duplicate registration should have been rejected')

    rows = catalog.load_table(TABLE_ID).scan().to_arrow().sort_by('id').to_pylist()
    assert [row['id'] for row in rows] == [0, 1, 2, 3], rows
    logger.info('Read-back verified: %d rows %s', len(rows), [row['event'] for row in rows])


if __name__ == '__main__':
    main()
