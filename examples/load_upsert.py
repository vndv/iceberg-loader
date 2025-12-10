import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import pyarrow as pa

# Ensure parent directory (examples/) is on path
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from catalog import get_catalog

from iceberg_loader import LoaderConfig, load_data_to_iceberg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_upsert_example():
    catalog = get_catalog()
    table_identifier = ('default', 'example_upsert_users')

    # Cleanup if exists
    try:
        catalog.drop_table(table_identifier)
        logger.info('Dropped existing table %s', table_identifier)
    except Exception:
        pass

    # 1. Initial Load
    logger.info("--- Initial Load ---")
    initial_data = pa.Table.from_pydict(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "updated_at": [datetime.now(), datetime.now(), datetime.now()],
        }
    )

    config_overwrite = LoaderConfig(write_mode="overwrite")
    load_data_to_iceberg(initial_data, table_identifier, catalog, config=config_overwrite)

    table = catalog.load_table(table_identifier)
    rows = table.scan().to_arrow()
    logger.info("Initial rows: %d", len(rows))
    print(rows.to_pydict())

    time.sleep(1)  # Ensure timestamps are different

    # 3. Upsert (Update Bob, Insert David)
    logger.info("\n--- Upsert ---")
    upsert_data = pa.Table.from_pydict(
        {
            "id": [2, 4],
            "name": ["Bob Updated", "David"],
            "updated_at": [datetime.now(), datetime.now()],
        }
    )

    config_upsert = LoaderConfig(write_mode="upsert", join_cols=["id"])
    load_data_to_iceberg(upsert_data, table_identifier, catalog, config_upsert)

    rows_after = table.scan().to_arrow()
    logger.info("Rows after upsert: %d", len(rows_after))
    print(rows_after.to_pydict())


if __name__ == '__main__':
    run_upsert_example()
