import logging

import duckdb  # ty: ignore[unresolved-import]
import pyarrow as pa
from catalog import get_catalog

from iceberg_loader import LoaderConfig, load_data_to_iceberg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def build_orders() -> pa.Table:
    return pa.table(
        {
            'order_id': [1, 2, 3, 4, 5, 6],
            'customer_id': [10, 11, 10, 12, 11, 10],
            'amount': [120.0, 80.0, 200.0, 50.0, 320.0, 90.0],
        }
    )


def build_customers() -> pa.Table:
    return pa.table(
        {
            'customer_id': [10, 11, 12],
            'customer_name': ['Ada', 'Grace', 'Linus'],
        }
    )


def transform_orders() -> pa.Table:
    con = duckdb.connect()
    con.register('orders', build_orders())
    con.register('customers', build_customers())

    # DuckDB is both the source and the transform engine here. A plain SQL join + aggregate
    # closes the transform/dbt gap for this pipeline, and the result is already a PyArrow
    # table that iceberg-loader accepts unchanged.
    result = con.sql(
        """
        SELECT c.customer_name,
               count(*) AS orders_count,
               sum(o.amount) AS total_amount
        FROM orders AS o
        JOIN customers AS c USING (customer_id)
        GROUP BY c.customer_name
        ORDER BY c.customer_name
        """,
    ).arrow()
    # Older/newer DuckDB versions may return either a pa.Table or a RecordBatchReader.
    if isinstance(result, pa.RecordBatchReader):
        return result.read_all()
    return result


def main() -> None:
    catalog = get_catalog()
    table_id = ('default', 'duckdb_source_example')

    table_data = transform_orders()
    logger.info('Transformed %d customer rows in DuckDB', table_data.num_rows)

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
