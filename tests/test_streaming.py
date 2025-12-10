import io
import unittest
from unittest.mock import MagicMock

import pyarrow as pa
from iceberg_loader import IcebergLoader
from pyiceberg.exceptions import NoSuchTableError


class TestStreaming(unittest.TestCase):
    def test_load_ipc_stream(self):
        schema = pa.schema([pa.field('id', pa.int64()), pa.field('value', pa.string())])
        sink = io.BytesIO()
        with pa.ipc.new_stream(sink, schema) as writer:
            for i in range(2):
                batch = pa.RecordBatch.from_pydict(
                    {'id': [i * 2, i * 2 + 1], 'value': [f'v{i}a', f'v{i}b']},
                    schema=schema,
                )
                writer.write_batch(batch)
        sink.seek(0)

        mock_table = MagicMock()
        mock_txn = mock_table.transaction.return_value.__enter__.return_value

        mock_catalog = MagicMock()
        mock_catalog.load_table.side_effect = [NoSuchTableError, mock_table]

        loader = IcebergLoader(mock_catalog)
        expected_iceberg_schema = loader.schema_manager._arrow_to_iceberg(schema)
        mock_table.schema.return_value = expected_iceberg_schema
        result = loader.load_ipc_stream(
            stream_source=sink,
            table_identifier=('default', 'streaming_test'),
            write_mode='append',
        )

        mock_catalog.create_table.assert_called_once()
        self.assertEqual(mock_txn.append.call_count, 2)
        self.assertEqual(result['rows_loaded'], 4)
        self.assertEqual(result['batches_processed'], 2)


if __name__ == '__main__':
    unittest.main()
