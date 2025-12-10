import logging
import sys
from pathlib import Path

# Ensure parent directory (examples/) is on path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from catalog import get_catalog

from iceberg_loader import load_data_to_iceberg
from iceberg_loader.arrow_utils import create_arrow_table_from_data
from optional.rest_adapter import RestAdapter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    catalog = get_catalog()
    adapter = RestAdapter()

    endpoint_list = ['customers', 'orders', 'items', 'products', 'supplies', 'stores']

    for endpoint in endpoint_list:
        logger.info('Loading data from endpoint: %s', endpoint)

        # Get data from API (generator that yields list of dicts)
        endpoint_data_generator = adapter.get_data(endpoint)

        # Convert generator to list (since API returns small datasets)
        endpoint_data = []
        for batch in endpoint_data_generator:
            endpoint_data.extend(batch if isinstance(batch, list) else [batch])

        logger.info('Fetched %d records from %s', len(endpoint_data), endpoint)

        # Convert to Arrow table
        arrow_table = create_arrow_table_from_data(endpoint_data)

        # Load to Iceberg using 'default' namespace
        result = load_data_to_iceberg(
            table_data=arrow_table,
            table_identifier=('default', endpoint),
            catalog=catalog,
            write_mode='append',
            schema_evolution=True,
        )

        logger.info('Loaded endpoint %s: %s', endpoint, result)


if __name__ == '__main__':
    main()
