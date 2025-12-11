import io
import logging

from catalog import get_catalog
from pyiceberg.exceptions import NoSuchTableError

from iceberg_loader import LoaderConfig, load_ipc_stream_to_iceberg

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def run_stream_load():
    import pyarrow as pa

    catalog = get_catalog()
    config = LoaderConfig(write_mode='append', commit_interval=5)
    table_id = ('default', 'stream_test')

    try:
        catalog.drop_table(table_id)
        logger.info('Dropped existing table %s', table_id)
    except NoSuchTableError:
        pass

    logger.info('Generating IPC stream...')

    sink = io.BytesIO()
    schema = pa.schema([pa.field('id', pa.int64()), pa.field('value', pa.string())])
    target_mb = 500
    target_bytes = target_mb * 1024 * 1024
    batch_rows = 200_000
    written_batches = 0

    with pa.ipc.new_stream(sink, schema) as writer:
        while sink.getbuffer().nbytes < target_bytes:
            base = written_batches * batch_rows
            batch_data = {
                'id': list(range(base, base + batch_rows)),
                'value': [f'val_{written_batches}_{j}' for j in range(batch_rows)],
            }
            batch = pa.RecordBatch.from_pydict(batch_data, schema=schema)
            writer.write_batch(batch)
            written_batches += 1
            if written_batches % 5 == 0:
                logger.info('Stream size: %.1f MB', sink.getbuffer().nbytes / (1024 * 1024))

    sink.seek(0)
    logger.info('Final stream size: %.1f MB (%s batches)', sink.getbuffer().nbytes / (1024 * 1024), written_batches)

    logger.info('Loading from IPC stream source...')

    result = load_ipc_stream_to_iceberg(
        stream_source=sink,
        table_identifier=table_id,
        catalog=catalog,
        config=config,
    )

    logger.info('Load result: %s', result)

    table = catalog.load_table(table_id)
    count = table.scan().to_arrow().num_rows
    logger.info('Verified rows in table: %s (expected: %s)', count, written_batches * batch_rows)


if __name__ == '__main__':
    run_stream_load()
