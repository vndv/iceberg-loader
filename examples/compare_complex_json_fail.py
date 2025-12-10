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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_comparison():
    pa = import_module('pyarrow')
    loader_mod = import_module('iceberg_loader')
    loader_config_cls = loader_mod.LoaderConfig
    load_data_to_iceberg = loader_mod.load_data_to_iceberg
    create_arrow_table_from_data = import_module('iceberg_loader.arrow_utils').create_arrow_table_from_data
    catalog = get_catalog()
    table_id = ('default', 'comparison_complex_json')

    # Data with inconsistent types in 'complex_field':
    # 1. Dict with 'a', 'b'
    # 2. Dict with 'a', 'b', 'c' (schema mismatch if strict)
    # 3. List (complete type mismatch with Dict)
    data = [
        {'id': 1, 'complex_field': {'a': 1, 'b': 'nested'}},
        {'id': 2, 'complex_field': {'a': 2, 'b': 'another', 'c': [1, 2]}},
        {'id': 3, 'complex_field': [1, 2, 3]},
    ]

    logger.info("--- 1. Attempting standard PyArrow inference (Standard Approach) ---")
    try:
        # This is what users typically try first
        _ = pa.Table.from_pylist(data)
        logger.warning("Unexpected: pa.Table.from_pylist succeeded (it usually fails with mixed types).")
    except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
        logger.info("SUCCESS: PyArrow failed as expected with: %s", e)
        logger.info("Reason: PyArrow cannot infer a single schema for mixed Dict/List/Struct types.")

    logger.info("\n--- 2. Attempting iceberg-loader (Our Solution) ---")
    try:
        # 1. Convert using our utility
        arrow_table = create_arrow_table_from_data(data)
        logger.info("Created Arrow table with schema:\n%s", arrow_table.schema)

        # 2. Load to Iceberg
        config = loader_config_cls(write_mode='overwrite', schema_evolution=True)
        load_data_to_iceberg(
            table_data=arrow_table,
            table_identifier=table_id,
            catalog=catalog,
            config=config,
        )
        logger.info("Successfully loaded data to Iceberg table '%s'", table_id)

        # 3. Verify
        table = catalog.load_table(table_id)
        logger.info("Table schema:\n%s", table.schema())
        rows = table.scan().to_arrow().to_pylist()
        logger.info("Loaded rows: %s", rows)

    except Exception as e:
        logger.error("iceberg-loader failed: %s", e)
        raise


if __name__ == '__main__':
    run_comparison()

