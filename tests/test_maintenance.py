from datetime import datetime, timezone

from iceberg_loader.maintenance import SnapshotMaintenance, expire_snapshots


class FakeSnapshot:
    def __init__(self, snapshot_id: int, timestamp_ms: int) -> None:
        self.snapshot_id = snapshot_id
        self.timestamp_ms = timestamp_ms


class FakeExpire:
    def __init__(self, table: 'FakeTable') -> None:
        self.table = table
        self.cutoff_ms: int | None = None

    def older_than(self, dt: datetime) -> 'FakeExpire':
        self.cutoff_ms = int(dt.timestamp() * 1000)
        return self

    def commit(self) -> None:
        if self.cutoff_ms is None:
            return
        self.table.snapshots_list = [snap for snap in self.table.snapshots_list if snap.timestamp_ms >= self.cutoff_ms]


class FakeMaintenanceOps:
    def __init__(self, table: 'FakeTable') -> None:
        self.table = table

    def expire_snapshots(self) -> FakeExpire:
        return FakeExpire(self.table)


class FakeTable:
    def __init__(self, snapshots: list[FakeSnapshot]) -> None:
        self.snapshots_list = snapshots
        self.maintenance = FakeMaintenanceOps(self)

    def refresh(self) -> None:
        return

    def snapshots(self):
        return list(self.snapshots_list)

    def current_snapshot(self):
        return self.snapshots_list[-1] if self.snapshots_list else None

    def name(self):
        return 'fake_table'


def test_expire_keep_last_one():
    snapshots = [
        FakeSnapshot(1, 1_000),
        FakeSnapshot(2, 2_000),
        FakeSnapshot(3, 3_000),
    ]
    table = FakeTable(snapshots)
    maint = SnapshotMaintenance()

    maint.expire_snapshots(table, keep_last=1)

    remaining = table.snapshots_list
    assert len(remaining) == 1
    assert remaining[0].snapshot_id == 3


def test_expire_older_than_ms():
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    old = now_ms - 10_000
    mid = now_ms - 5_000
    new = now_ms

    snapshots = [
        FakeSnapshot(1, old),
        FakeSnapshot(2, mid),
        FakeSnapshot(3, new),
    ]
    table = FakeTable(snapshots)
    maint = SnapshotMaintenance()

    cutoff = now_ms - 6_000
    maint.expire_snapshots(table, keep_last=10, older_than_ms=cutoff)

    remaining = table.snapshots_list
    assert len(remaining) == 2
    ids = {s.snapshot_id for s in remaining}
    assert ids == {2, 3}


def test_expire_snapshots_helper():
    snapshots = [
        FakeSnapshot(1, 1_000),
        FakeSnapshot(2, 2_000),
        FakeSnapshot(3, 3_000),
    ]
    table = FakeTable(snapshots)

    expire_snapshots(table, keep_last=1)

    assert len(table.snapshots_list) == 1
    assert table.snapshots_list[0].snapshot_id == 3


def test_no_snapshots():
    table = FakeTable([])
    SnapshotMaintenance().expire_snapshots(table, keep_last=1)
    assert table.snapshots_list == []


def test_keep_last_negative_skips():
    snapshots = [
        FakeSnapshot(1, 1_000),
        FakeSnapshot(2, 2_000),
    ]
    table = FakeTable(snapshots)
    SnapshotMaintenance().expire_snapshots(table, keep_last=-1)
    assert len(table.snapshots_list) == 2
