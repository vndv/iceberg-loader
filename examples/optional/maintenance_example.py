import logging
import sys
from pathlib import Path

# Ensure parent directory (examples/) is on path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from catalog import get_catalog

from iceberg_loader import expire_snapshots

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)


def main() -> None:
    catalog = get_catalog()
    table_id = ('default', 'advanced_s1_initial_append')

    logger.info('Running snapshot maintenance for table: %s', table_id)
    table = catalog.load_table(table_id)

    expire_snapshots(table, keep_last=2)


if __name__ == '__main__':
    main()
