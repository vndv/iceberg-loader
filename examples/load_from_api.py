import logging

from catalog import get_catalog
from rest_adapter import RestAdapter

from iceberg_loader import LoaderConfig, load_data_to_iceberg
from iceberg_loader.arrow_utils import create_arrow_table_from_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    catalog = get_catalog()
    adapter = RestAdapter()

    # 1. Define different configurations
    config_overwrite = LoaderConfig(write_mode='overwrite', schema_evolution=True)
    config_upsert = LoaderConfig(write_mode='upsert', join_cols=['id'], schema_evolution=True)
    config_append = LoaderConfig(write_mode='append', schema_evolution=True)

    # 2. Map endpoints to configurations
    endpoint_configs = {
        'customers': config_overwrite,  # Full overwrite for customers
        'orders': config_upsert,  # Upsert for orders (update by id)
        'items': config_append,  # Append for items
        # Others will default to config_append
    }

    endpoint_list = ['customers', 'orders', 'items', 'products', 'supplies', 'stores']

    for endpoint in endpoint_list:
        logger.info('Loading data from endpoint: %s', endpoint)

        # 3. Select config dynamically
        # Default to append if not specified
        current_config = endpoint_configs.get(endpoint, config_append)
        logger.info('Using config mode: %s for %s', current_config.write_mode, endpoint)

        endpoint_data_generator = adapter.get_data(endpoint)

        endpoint_data = []
        for batch in endpoint_data_generator:
            endpoint_data.extend(batch if isinstance(batch, list) else [batch])

        logger.info('Fetched %d records from %s', len(endpoint_data), endpoint)

        if not endpoint_data:
            logger.warning('No data for %s, skipping', endpoint)
            continue

        arrow_table = create_arrow_table_from_data(endpoint_data)

        result = load_data_to_iceberg(
            table_data=arrow_table,
            table_identifier=('default', endpoint),
            catalog=catalog,
            config=current_config,
        )

        logger.info('Loaded endpoint %s: %s', endpoint, result)


if __name__ == '__main__':
    main()
