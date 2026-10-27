import logging

import polars as pl  # ty: ignore[unresolved-import]
from catalog import get_catalog

from iceberg_loader import LoaderConfig, load_data_to_iceberg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def transform_orders() -> pl.DataFrame:
    orders = pl.DataFrame(
        {
            'order_id': [1, 2, 3, 4, 5, 6],
            'customer_id': [10, 11, 10, 12, 11, 10],
            'amount': [120.0, 80.0, 200.0, 50.0, 320.0, 90.0],
        }
    )
    customers = pl.DataFrame(
        {
            'customer_id': [10, 11, 12],
            'customer_name': ['Ada', 'Grace', 'Linus'],
        }
    )

    # Polars does the join and aggregation; to_arrow() hands over a PyArrow table whose
    # large_string columns iceberg-loader maps to Iceberg string.
    return (
        orders.join(customers, on='customer_id', how='left')
        .group_by('customer_name')
        .agg(
            pl.len().alias('orders_count'),
            pl.col('amount').sum().alias('total_amount'),
        )
        .sort('customer_name')
    )


def main() -> None:
    catalog = get_catalog()
    table_id = ('default', 'polars_source_example')

    table_data = transform_orders().to_arrow()
    logger.info('Transformed %d customer rows in Polars', table_data.num_rows)

    result = load_data_to_iceberg(
        table_data=table_data,
        table_identifier=table_id,
        catalog=catalog,
        config=LoaderConfig(write_mode='overwrite'),
    )
    logger.info('Loaded %s: %s', table_id, result)

    rows = catalog.load_table(table_id).scan().to_arrow().to_pylist()
    assert len(rows) == 3, f'Expected 3 customer rows, got {len(rows)}'
    ada = next(row for row in rows if row['customer_name'] == 'Ada')
    assert ada['orders_count'] == 3, f"Expected Ada's 3 orders, got {ada['orders_count']}"
    assert ada['total_amount'] == 410.0, f"Expected Ada's total 410.0, got {ada['total_amount']}"
    logger.info('Read-back verified: %s', rows)


if __name__ == '__main__':
    main()
