import logging
from importlib import import_module
from pathlib import Path
import sys

BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BASE_DIR / 'src'))
sys.path.insert(0, str(BASE_DIR / 'examples'))

from catalog import get_catalog  # noqa: E402

loader_mod = import_module('iceberg_loader')
LoaderConfig = loader_mod.LoaderConfig
load_data_to_iceberg = loader_mod.load_data_to_iceberg
create_arrow_table_from_data = import_module('iceberg_loader.arrow_utils').create_arrow_table_from_data
NoSuchTableError = import_module('pyiceberg.exceptions').NoSuchTableError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def drop_if_exists(catalog, table_id):
    try:
        catalog.drop_table(table_id)
        logger.info('Dropped existing table %s', table_id)
    except NoSuchTableError:
        return


def scenario_initial_append(catalog):
    table_id = ('default', 'advanced_s1_initial_append')
    # drop_if_exists(catalog, table_id)

    data_day_1 = [
        {'id': 1, 'category': 'A', 'ts': '2023-01-01', 'value': 100},
        {'id': 2, 'category': 'B', 'ts': '2023-01-01', 'value': 200},
    ]
    table_arrow = create_arrow_table_from_data(data_day_1)

    config = LoaderConfig(write_mode='append', partition_col='ts', schema_evolution=True)
    load_data_to_iceberg(table_data=table_arrow, table_identifier=table_id, catalog=catalog, config=config)
    verify_table(catalog, table_id, expected_rows=2)


def scenario_append_new_partition(catalog):
    table_id = ('default', 'advanced_s2_append_partition')
    # drop_if_exists(catalog, table_id)

    # Initial day 1
    data_day_1 = [
        {'id': 1, 'category': 'A', 'ts': '2023-01-01', 'value': 100},
        {'id': 2, 'category': 'B', 'ts': '2023-01-01', 'value': 200},
    ]
    config = LoaderConfig(write_mode='append', partition_col='month(ts)', schema_evolution=True)
    load_data_to_iceberg(
        table_data=create_arrow_table_from_data(data_day_1),
        table_identifier=table_id,
        catalog=catalog,
        config=config,
    )
    # Append day 2
    data_day_2 = [
        {'id': 3, 'category': 'A', 'ts': '2023-01-02', 'value': 150},
    ]
    load_data_to_iceberg(
        table_data=create_arrow_table_from_data(data_day_2),
        table_identifier=table_id,
        catalog=catalog,
        config=LoaderConfig(write_mode='append'),
    )
    verify_table(catalog, table_id, expected_rows=3)


def scenario_idempotent_replace_partition(catalog):
    table_id = ('default', 'advanced_s3_idempotent_replace')
    # drop_if_exists(catalog, table_id)

    # Base data day1+day2
    base_data = [
        {'id': 1, 'category': 'A', 'ts': '2023-01-01', 'value': 100},
        {'id': 2, 'category': 'B', 'ts': '2023-01-01', 'value': 200},
        {'id': 3, 'category': 'A', 'ts': '2023-01-02', 'value': 150},
    ]
    config_base = LoaderConfig(write_mode='append', partition_col='ts', schema_evolution=True)
    load_data_to_iceberg(
        table_data=create_arrow_table_from_data(base_data),
        table_identifier=table_id,
        catalog=catalog,
        config=config_base,
    )

    # Corrected day1
    corrected_day1 = [
        {'id': 1, 'category': 'A', 'ts': '2023-01-01', 'value': 999},
        {'id': 2, 'category': 'B', 'ts': '2023-01-01', 'value': 200},
    ]
    config_replace = LoaderConfig(write_mode='append', replace_filter="ts == '2023-01-01'")
    load_data_to_iceberg(
        table_data=create_arrow_table_from_data(corrected_day1),
        table_identifier=table_id,
        catalog=catalog,
        config=config_replace,
    )
    verify_table(catalog, table_id, expected_rows=3)


def scenario_schema_evolution(catalog):
    table_id = ('default', 'advanced_s4_schema_evolution')
    drop_if_exists(catalog, table_id)

    base_data = [
        {'id': 1, 'category': 'A', 'ts': '2023-01-01', 'value': 100},
    ]
    config_base = LoaderConfig(write_mode='append', partition_col='ts', schema_evolution=True)
    load_data_to_iceberg(
        table_data=create_arrow_table_from_data(base_data),
        table_identifier=table_id,
        catalog=catalog,
        config=config_base,
    )

    evolved = [
        {'id': 2, 'category': 'B', 'ts': '2023-01-02', 'value': 200, 'new_col': 'extra_info'},
    ]
    load_data_to_iceberg(
        table_data=create_arrow_table_from_data(evolved),
        table_identifier=table_id,
        catalog=catalog,
        config=LoaderConfig(write_mode='append', schema_evolution=True),
    )
    verify_table(catalog, table_id, expected_rows=2)

    table = catalog.load_table(table_id)
    if 'new_col' in table.schema().column_names:
        logger.info("SUCCESS: Schema evolved, 'new_col' found.")
    else:
        logger.error("FAILURE: 'new_col' missing in schema.")


def scenario_full_overwrite(catalog):
    table_id = ('default', 'advanced_s5_full_overwrite')
    # drop_if_exists(catalog, table_id)

    initial = [
        {'id': 1, 'category': 'A', 'ts': '2023-01-01', 'value': 100},
        {'id': 2, 'category': 'B', 'ts': '2023-01-02', 'value': 200},
    ]
    config_base = LoaderConfig(write_mode='append', partition_col='ts', schema_evolution=True)
    load_data_to_iceberg(
        table_data=create_arrow_table_from_data(initial),
        table_identifier=table_id,
        catalog=catalog,
        config=config_base,
    )

    replace_all = [
        {'id': 99, 'category': 'Z', 'ts': '2023-12-31', 'value': 0, 'new_col': 'reset'},
    ]
    load_data_to_iceberg(
        table_data=create_arrow_table_from_data(replace_all),
        table_identifier=table_id,
        catalog=catalog,
        config=LoaderConfig(write_mode='overwrite', schema_evolution=True),
    )
    verify_table(catalog, table_id, expected_rows=1)


def run_scenarios():
    catalog = get_catalog()
    logger.info('\n--- Scenario 1: Initial Load (Append) ---')
    scenario_initial_append(catalog)

    logger.info('\n--- Scenario 2: Append data for new partition ---')
    scenario_append_new_partition(catalog)

    logger.info('\n--- Scenario 3: Idempotent Overwrite (Replace Partition Day 1) ---')
    scenario_idempotent_replace_partition(catalog)

    logger.info('\n--- Scenario 4: Schema Evolution (Add Column) ---')
    scenario_schema_evolution(catalog)

    logger.info('\n--- Scenario 5: Full Overwrite ---')
    scenario_full_overwrite(catalog)


def verify_table(catalog, table_id, expected_rows):
    table = catalog.load_table(table_id)
    rows = table.scan().to_arrow().num_rows
    if rows == expected_rows:
        logger.info('Verified: Table %s has %s rows (Expected: %s)', table_id, rows, expected_rows)
    else:
        logger.error('Mismatch: Table %s has %s rows, expected %s', table_id, rows, expected_rows)


if __name__ == '__main__':
    run_scenarios()
