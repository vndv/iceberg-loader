import logging
from importlib import import_module
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(BASE_DIR / 'src'))
sys.path.insert(0, str(BASE_DIR / 'examples'))

from catalog import get_catalog  # noqa: E402
from optional.rest_adapter import RestAdapter  # noqa: E402

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    catalog = get_catalog()
    adapter = RestAdapter()
    loader_mod = import_module('iceberg_loader')
    loader_config_cls = loader_mod.LoaderConfig
    load_data_to_iceberg = loader_mod.load_data_to_iceberg
    create_arrow_table_from_data = import_module('iceberg_loader.arrow_utils').create_arrow_table_from_data
    config = loader_config_cls(write_mode='overwrite', schema_evolution=True)

    endpoint_list = ['customers', 'orders', 'items', 'products', 'supplies', 'stores']

    for endpoint in endpoint_list:
        logger.info('Loading data from endpoint: %s', endpoint)

        endpoint_data_generator = adapter.get_data(endpoint)

        endpoint_data = []
        for batch in endpoint_data_generator:
            endpoint_data.extend(batch if isinstance(batch, list) else [batch])

        logger.info('Fetched %d records from %s', len(endpoint_data), endpoint)

        arrow_table = create_arrow_table_from_data(endpoint_data)

        result = load_data_to_iceberg(
            table_data=arrow_table,
            table_identifier=('default', endpoint),
            catalog=catalog,
            config=config,
        )

        logger.info('Loaded endpoint %s: %s', endpoint, result)


if __name__ == '__main__':
    main()
