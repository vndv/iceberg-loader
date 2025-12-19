from datetime import datetime
from unittest.mock import MagicMock, patch

import pyarrow as pa
from pyiceberg.exceptions import NoSuchTableError

from iceberg_loader import LoaderConfig, load_data_to_iceberg
from iceberg_loader.core.loader import IcebergLoader


def test_quickstart_smoke_example() -> None:
    """Quickstart-style smoke test using a mock catalog."""
    arrow_table = pa.Table.from_pydict(
        {
            'id': [1, 2, 3],
            'signup_date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        },
    )
    catalog = MagicMock()
    table = MagicMock()
    catalog.load_table.side_effect = [NoSuchTableError, table]

    loader = IcebergLoader(catalog)
    adjusted_schema = loader.schema_manager._adjust_schema_for_partitioning(
        arrow_table.schema,
        'day(signup_date)',
    )
    expected_schema = loader.schema_manager._arrow_to_iceberg(adjusted_schema)
    table.schema.return_value = expected_schema
    table.current_snapshot.return_value = MagicMock(snapshot_id=1)
    table.location.return_value = 'memory://table'

    config = LoaderConfig(
        write_mode='append',
        partition_col='day(signup_date)',
        load_timestamp=datetime(2024, 1, 10),
    )

    extended_schema = adjusted_schema.append(pa.field(config.load_ts_col, pa.timestamp('us')))

    with patch.object(loader.schema_manager, 'get_arrow_schema', return_value=extended_schema):
        result = load_data_to_iceberg(arrow_table, ('default', 'example'), catalog, config=config)

    create_kwargs = catalog.create_table.call_args.kwargs
    partition_spec = create_kwargs.get('partition_spec')
    assert partition_spec is not None
    assert partition_spec.fields[0].name == 'signup_date_day'
    assert result['rows_loaded'] == 3
