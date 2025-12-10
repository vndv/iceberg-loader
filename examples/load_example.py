import logging

from catalog import get_catalog

from iceberg_loader import LoaderConfig, load_data_to_iceberg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_example():
    catalog = get_catalog()
    logger.info('Connected to catalog: %s', catalog)

    # 2. Prepare complex JSON data
    # Simulating data with nested structures (list of dicts, nested dicts)
    raw_data = [
        {
            'user_id': 1,
            'name': 'Alice',
            'events': [{'type': 'login', 'ts': 1672531200}, {'type': 'view_product', 'product_id': 123}],
            'attributes': {'country': 'US', 'premium': True},
            'signup_date': '2023-01-01',
        },
        {
            'user_id': 2,
            'name': 'Bob',
            'events': [{'type': 'logout', 'ts': 1672617600}],
            'attributes': {'country': 'UK', 'premium': False},
            'signup_date': '2023-01-01',
        },
        {
            'user_id': 3,
            'name': 'Charlie',
            'events': [],
            'attributes': None,  # Nullable nested field
            'signup_date': '2023-01-02',
        },
    ]

    from iceberg_loader.arrow_utils import create_arrow_table_from_data

    arrow_table = create_arrow_table_from_data(raw_data)

    logger.info('Prepared Arrow table with schema:\n%s', arrow_table.schema)

    # 3. Load data into Iceberg
    table_id = ('default', 'complex_users')

    # Clean up table if exists to start fresh
    try:
        catalog.drop_table(table_id)
        logger.info('Dropped existing table %s', table_id)
    except Exception:
        pass

    config = LoaderConfig(
        write_mode='append',
        partition_col='signup_date',
        schema_evolution=True,
    )

    result = load_data_to_iceberg(
        table_data=arrow_table,
        table_identifier=table_id,
        catalog=catalog,
        config=config,
    )

    logger.info('Load result: %s', result)

    # 4. Verify (Optional read back)
    try:
        table = catalog.load_table(table_id)
        scanned = table.scan().to_arrow()
        logger.info('Read back %d rows from Iceberg', len(scanned))
        logger.info('First row: %s', scanned.to_pylist()[0])
        logger.info('Partition spec: %s', table.spec())
    except Exception as e:
        logger.error('Failed to read back table: %s', e)


if __name__ == '__main__':
    run_example()
