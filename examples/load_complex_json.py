import json
import logging
from importlib import import_module
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR / 'src'))
sys.path.insert(0, str(BASE_DIR / 'examples'))

from catalog import get_catalog  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def drop_if_exists(catalog, table_id):
    no_such_table_error = import_module('pyiceberg.exceptions').NoSuchTableError
    try:
        catalog.drop_table(table_id)
        logger.info('Dropped existing table %s', table_id)
    except no_such_table_error:
        return


def run_complex_load():
    loader_mod = import_module('iceberg_loader')
    LoaderConfig = loader_mod.LoaderConfig
    load_data_to_iceberg = loader_mod.load_data_to_iceberg
    create_arrow_table_from_data = import_module('iceberg_loader.arrow_utils').create_arrow_table_from_data
    catalog = get_catalog()
    table_id = ('default', 'complex_json_test')
    # drop_if_exists(catalog, table_id)

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
        logger.info('Row: %s', row)
        # Check if complex_field is a string
        val = row['complex_field']
        if isinstance(val, str):
            logger.info('  -> complex_field is STRING as expected. Parsed: %s', json.loads(val))
        else:
            logger.error('  -> complex_field is NOT a string! Type: %s', type(val))


if __name__ == '__main__':
    run_complex_load()
