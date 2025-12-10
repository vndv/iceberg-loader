import json
import logging
import sys
from pathlib import Path

# Ensure parent directory (examples/) is on path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from catalog import get_catalog

from iceberg_loader import LoaderConfig, load_data_to_iceberg
from iceberg_loader.arrow_utils import create_arrow_table_from_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def drop_if_exists(catalog, table_id):
    try:
        catalog.drop_table(table_id)
        logger.info('Dropped existing table %s', table_id)
    except Exception:
        pass


def run_complex_load():
    catalog = get_catalog()
    table_id = ('default', 'complex_json_test')
    drop_if_exists(catalog, table_id)

    data = [
        {'id': 1, 'complex_field': {'a': 1, 'b': 'nested'}},
        {'id': 2, 'complex_field': {'a': 2, 'b': 'another', 'c': [1, 2]}},
        {'id': 3, 'complex_field': [1, 2, 3]},
    ]

    logger.info('Original Data:')
    for row in data:
        logger.info(row)

    logger.info('\nConverting to Arrow using create_arrow_table_from_data...')
    arrow_table = create_arrow_table_from_data(data)

    logger.info('Arrow Schema:')
    logger.info(arrow_table.schema)

    logger.info('Arrow Data (first 3 rows):')
    logger.info(arrow_table.to_pydict())

    logger.info('\nLoading to Iceberg...')
    config = LoaderConfig(write_mode='append', schema_evolution=True)
    load_data_to_iceberg(table_data=arrow_table, table_identifier=table_id, catalog=catalog, config=config)

    logger.info('\nVerifying data in Iceberg...')
    table = catalog.load_table(table_id)
    result_arrow = table.scan().to_arrow()
    result_data = result_arrow.to_pylist()

    for row in result_data:
        logger.info(f'Row: {row}')
        # Check if complex_field is a string
        val = row['complex_field']
        if isinstance(val, str):
            logger.info(f'  -> complex_field is STRING as expected. Parsed: {json.loads(val)}')
        else:
            logger.error(f'  -> complex_field is NOT a string! Type: {type(val)}')


if __name__ == '__main__':
    run_complex_load()
