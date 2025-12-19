from datetime import datetime
from typing import Any

from pyiceberg.exceptions import CommitFailedException as IcebergError

from iceberg_loader.services.logging import logger


class SnapshotMaintenance:
    """Manages Iceberg table snapshot maintenance operations."""

    def expire_snapshots(self, table: Any, keep_last: int = 1, older_than_ms: int | None = None) -> None:
        """Expire old snapshots to prevent metadata issues."""
        try:
            table.refresh()

            snapshots = list(table.snapshots())
            logger.info('Found %d snapshots', len(snapshots))

            if len(snapshots) == 0:
                logger.info('No snapshots found, nothing to fix')
                return

            self._log_snapshot_details(table, snapshots)

            expire = table.maintenance.expire_snapshots()
            should_expire = False

            if older_than_ms is not None:
                should_expire = True
                cutoff_datetime = datetime.fromtimestamp(older_than_ms / 1000.0)
                logger.info('Expiring snapshots older than %s (ms=%d)', cutoff_datetime, older_than_ms)
                expire = expire.older_than(cutoff_datetime)
            else:
                should_expire, cutoff_dt = self._determine_cutoff(snapshots, keep_last)
                if should_expire and cutoff_dt:
                    expire = expire.older_than(cutoff_dt)

            if not should_expire:
                return

            self._commit_expiration(table, expire, len(snapshots))

        except (IcebergError, OSError, ValueError, RuntimeError) as e:
            logger.warning('Failed to expire snapshots for table %s: %s', table.name(), e)

    def _log_snapshot_details(self, table: Any, snapshots: list[Any]) -> None:
        sorted_snapshots = sorted(snapshots, key=lambda s: s.timestamp_ms)
        current_snapshot = table.current_snapshot()
        logger.info('Snapshot details (sorted by timestamp):')
        for i, snap in enumerate(sorted_snapshots):
            is_current = current_snapshot and snap.snapshot_id == current_snapshot.snapshot_id
            marker = ' <-- CURRENT' if is_current else ''
            logger.info('  %d. ID=%s, timestamp=%s%s', i + 1, snap.snapshot_id, snap.timestamp_ms, marker)

    def _determine_cutoff(self, snapshots: list[Any], keep_last: int) -> tuple[bool, datetime | None]:
        if keep_last < 0:
            logger.info('keep_last < 0 specified, skipping expiration')
            return False, None

        if len(snapshots) <= keep_last:
            logger.info('Table has %d snapshots, keep_last=%d → nothing to expire', len(snapshots), keep_last)
            return False, None

        sorted_snapshots = sorted(snapshots, key=lambda s: s.timestamp_ms)
        cutoff_snapshot = sorted_snapshots[-keep_last]
        cutoff_datetime = datetime.fromtimestamp((cutoff_snapshot.timestamp_ms - 1) / 1000.0)

        logger.info(
            'Expiring snapshots older than %s to keep last %d snapshot(s)',
            cutoff_datetime,
            keep_last,
        )
        return True, cutoff_datetime

    def _commit_expiration(self, table: Any, expire_action: Any, before_count: int) -> None:
        expire_action.commit()
        table.refresh()
        remaining_snapshots = list(table.snapshots())
        after_count = len(remaining_snapshots)
        logger.info('Successfully expired snapshots: %d removed, %d remaining', before_count - after_count, after_count)


def expire_snapshots(table: Any, keep_last: int = 1, older_than_ms: int | None = None) -> None:
    """Convenience function to expire snapshots without instantiating SnapshotMaintenance."""
    SnapshotMaintenance().expire_snapshots(table, keep_last=keep_last, older_than_ms=older_than_ms)
