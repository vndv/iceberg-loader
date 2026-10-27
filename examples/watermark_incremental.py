import datetime as dt
import logging

import duckdb  # ty: ignore[unresolved-import]
import pyarrow as pa
from catalog import get_catalog

from iceberg_loader import LoaderConfig, get_high_watermark, load_data_to_iceberg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TABLE_ID = ('default', 'watermark_incremental_example')
INITIAL_ORDERS = [
    (1, dt.datetime(2024, 1, 1, 9, 0), 10.0),
    (2, dt.datetime(2024, 1, 1, 12, 0), 20.0),
    (3, dt.datetime(2024, 1, 2, 8, 0), 30.0),
]
NEW_ORDERS = [
    (4, dt.datetime(2024, 1, 2, 15, 0), 40.0),
    (5, dt.datetime(2024, 1, 3, 7, 0), 50.0),
]


def connect_source() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB source with the initial set of orders."""
    source = duckdb.connect()
    source.execute('CREATE TABLE orders (id BIGINT, updated_at TIMESTAMP, amount DOUBLE)')
    insert_orders(source, INITIAL_ORDERS)
    return source


def insert_orders(source: duckdb.DuckDBPyConnection, orders: list[tuple[int, dt.datetime, float]]) -> None:
    source.executemany('INSERT INTO orders VALUES (?, ?, ?)', orders)


def query_arrow(source: duckdb.DuckDBPyConnection, query: str, params: list[object] | None = None) -> pa.Table:
    result = source.sql(query, params=params).arrow()
    # Older/newer DuckDB versions may return either a pa.Table or a RecordBatchReader.
    if isinstance(result, pa.RecordBatchReader):
        return result.read_all()
    return result


def main() -> None:
    catalog = get_catalog()
    source = connect_source()

    # Run 1: a full load resets the destination, so re-running this example stays deterministic.
    initial_orders = query_arrow(source, 'SELECT * FROM orders ORDER BY id')
    result = load_data_to_iceberg(
        initial_orders,
        TABLE_ID,
        catalog,
        LoaderConfig(write_mode='overwrite'),
    )
    logger.info('Run 1 (overwrite) loaded %d orders: %s', initial_orders.num_rows, result)

    # Run 2: read the watermark from the destination and filter the source with "> watermark".
    watermark = get_high_watermark(TABLE_ID, 'updated_at', catalog)
    logger.info('Watermark after run 1: %s', watermark)
    assert watermark == dt.datetime(2024, 1, 2, 8, 0), f'Unexpected watermark: {watermark}'

    insert_orders(source, NEW_ORDERS)
    new_orders = query_arrow(
        source,
        'SELECT * FROM orders WHERE updated_at > ? ORDER BY id',
        [watermark],
    )
    logger.info('Source returned %d orders newer than the watermark', new_orders.num_rows)
    assert new_orders.num_rows == len(NEW_ORDERS), f'Expected {len(NEW_ORDERS)} new orders, got {new_orders.num_rows}'

    result = load_data_to_iceberg(
        new_orders,
        TABLE_ID,
        catalog,
        LoaderConfig(write_mode='append'),
    )
    logger.info('Run 2 (append) loaded %d new orders: %s', new_orders.num_rows, result)

    rows = catalog.load_table(TABLE_ID).scan().to_arrow().to_pylist()
    order_ids = sorted(row['id'] for row in rows)
    assert order_ids == [1, 2, 3, 4, 5], f'Expected all five orders exactly once, got {order_ids}'
    logger.info('Read-back verified: %d orders %s', len(rows), order_ids)


if __name__ == '__main__':
    main()
