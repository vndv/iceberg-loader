import logging
from datetime import datetime
from typing import Any, Optional

try:
    from pyiceberg.exceptions import IcebergError
except ImportError:  # pragma: no cover - fallback for environments without pyiceberg
    class IcebergError(Exception):  # type: ignore[override]
        """Fallback when pyiceberg is not installed."""

logger = logging.getLogger(__name__)


class SnapshotMaintenance:
    """Handles maintenance tasks for Iceberg tables, such as expiring snapshots."""

    def expire_snapshots(self, table: Any, keep_last: int = 1, older_than_ms: Optional[int] = None) -> None:
        """
        Expire old snapshots to prevent metadata issues.

        Args:
            table: Iceberg table instance.
            keep_last: How many most recent snapshots to keep (default: 1).
            older_than_ms: Optional timestamp in milliseconds; expire snapshots strictly older than this moment.
                           If provided, it overrides keep_last logic.
        """
        try:
            table.refresh()

            snapshots = list(table.snapshots())
            logger.info('Found %d snapshots', len(snapshots))

            if len(snapshots) == 0:
                logger.info('No snapshots found, nothing to fix')
                return

            sorted_snapshots = sorted(snapshots, key=lambda s: s.timestamp_ms)

            logger.info('\nSnapshot details (sorted by timestamp):')
            for i, snap in enumerate(sorted_snapshots):
                is_current = table.current_snapshot() and snap.snapshot_id == table.current_snapshot().snapshot_id
                marker = ' <-- CURRENT' if is_current else ''
                logger.info('  %d. ID=%s, timestamp=%s%s', i + 1, snap.snapshot_id, snap.timestamp_ms, marker)

            expire = table.maintenance.expire_snapshots()

            # Determine cutoff strategy
            if older_than_ms is not None:
                cutoff_datetime = datetime.fromtimestamp(older_than_ms / 1000.0)
                logger.info('Expiring snapshots older than %s (ms=%d)', cutoff_datetime, older_than_ms)
                expire = expire.older_than(cutoff_datetime)
            else:
                if keep_last < 0:
                    logger.info('keep_last < 0 specified, skipping expiration')
                    return
                if len(snapshots) <= keep_last:
                    logger.info('Table has %d snapshots, keep_last=%d → nothing to expire', len(snapshots), keep_last)
                    return
                cutoff_snapshot = sorted_snapshots[-keep_last]
                cutoff_datetime = datetime.fromtimestamp((cutoff_snapshot.timestamp_ms - 1) / 1000.0)
                logger.info(
                    'Expiring snapshots older than %s to keep last %d snapshot(s)',
                    cutoff_datetime,
                    keep_last,
                )
                expire = expire.older_than(cutoff_datetime)

            before = len(snapshots)
            expire.commit()
            table.refresh()
            remaining_snapshots = list(table.snapshots())
            after = len(remaining_snapshots)
            logger.info('✓ Successfully expired snapshots: %d removed, %d remaining', before - after, after)

        except (IcebergError, OSError, ValueError, RuntimeError) as e:
            logger.warning('Failed to expire snapshots for table %s: %s', table.name(), e)


def expire_snapshots(table: Any, keep_last: int = 1, older_than_ms: Optional[int] = None) -> None:
    SnapshotMaintenance().expire_snapshots(table, keep_last=keep_last, older_than_ms=older_than_ms)
