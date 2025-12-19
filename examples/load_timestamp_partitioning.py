"""
Demonstrates safe partitioning by load timestamp and what happens with an invalid identity partition.

Prereqs: run the local stack from examples/ (docker-compose up -d), then execute:
    uv run python load_timestamp_partitioning.py
"""

import logging
from datetime import datetime

from pyiceberg.exceptions import NoSuchTableError
from pydantic import ValidationError

from catalog import get_catalog
from iceberg_loader import LoaderConfig, load_data_to_iceberg
from iceberg_loader.utils.arrow import create_arrow_table_from_data

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def show_invalid_identity_partition() -> None:
    """Identity on load_ts is rejected to avoid exploding partitions."""
    try:
        LoaderConfig(load_timestamp=datetime.now(), partition_col='_load_dttm')
    except ValidationError as exc:
        logger.info('Identity partition on _load_dttm is invalid:\n%s', exc)


def main() -> None:
    show_invalid_identity_partition()

    catalog = get_catalog()
    table_id = ('default', 'load_ts_partition_demo')

    try:
        catalog.drop_table(table_id)
        logger.info('Dropped existing table %s', table_id)
    except NoSuchTableError:
        pass

    data = create_arrow_table_from_data(
        [
            {'id': 1, 'event': 'a'},
            {'id': 2, 'event': 'b'},
            {'id': 3, 'event': 'c'},
        ],
    )

    config = LoaderConfig(
        write_mode='append',
        load_timestamp=datetime(2025, 1, 1, 12, 0, 0),
        partition_col='day(_load_dttm)',  # safe transform on the load timestamp
    )

    logger.info('Loading with partition_col=%s', config.partition_col)
    result = load_data_to_iceberg(data, table_id, catalog, config=config)
    logger.info('Load result: %s', result)

    table = catalog.load_table(table_id)
    logger.info('Partition spec: %s', table.spec())
    logger.info('Row count: %s', table.scan().to_arrow().num_rows)


if __name__ == '__main__':
    main()
