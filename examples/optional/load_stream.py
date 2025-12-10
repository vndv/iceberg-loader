import io
import logging

# Ensure parent directory (examples/) is on path
import sys
from pathlib import Path

import pyarrow as pa

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from catalog import get_catalog

from iceberg_loader import load_ipc_stream_to_iceberg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def drop_if_exists(catalog, table_id):
    try:
        catalog.drop_table(table_id)
        logger.info('Dropped existing table %s', table_id)
    except Exception:
        pass


def run_stream_load():
    catalog = get_catalog()
    table_id = ('default', 'stream_test')
    drop_if_exists(catalog, table_id)

    logger.info('Generating IPC stream...')

    sink = io.BytesIO()

    schema = pa.schema([pa.field('id', pa.int64()), pa.field('value', pa.string())])

    with pa.ipc.new_stream(sink, schema) as writer:
        for i in range(5):
            batch_data = {'id': [i * 10 + j for j in range(10)], 'value': [f'val_{i}_{j}' for j in range(10)]}
            batch = pa.RecordBatch.from_pydict(batch_data, schema=schema)
            writer.write_batch(batch)

    sink.seek(0)

    logger.info('Loading from IPC stream source...')

    result = load_ipc_stream_to_iceberg(
        stream_source=sink, table_identifier=table_id, catalog=catalog, write_mode='append'
    )

    logger.info(f'Load result: {result}')

    table = catalog.load_table(table_id)
    count = table.scan().to_arrow().num_rows
    logger.info(f'Verified rows in table: {count} (Expected: 50)')


if __name__ == '__main__':
    run_stream_load()
