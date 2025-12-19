from datetime import datetime

import pytest
from pydantic import ValidationError

from iceberg_loader.core.config import LoaderConfig


def test_upsert_cannot_use_replace_filter() -> None:
    with pytest.raises(ValidationError):
        LoaderConfig(write_mode='upsert', replace_filter='ts >= 0')


def test_partition_expression_must_be_valid() -> None:
    with pytest.raises(ValidationError):
        LoaderConfig(partition_col='month(')


def test_load_ts_identity_partition_rejected() -> None:
    with pytest.raises(ValidationError):
        LoaderConfig(partition_col='_load_dttm', load_timestamp=datetime.now())


def test_load_ts_transform_allowed() -> None:
    cfg = LoaderConfig(partition_col='day(_load_dttm)', load_timestamp=datetime.now())
    assert cfg.partition_col == 'day(_load_dttm)'


def test_load_ts_col_name_must_be_valid() -> None:
    with pytest.raises(ValidationError):
        LoaderConfig(load_ts_col='123bad')


def test_join_cols_cannot_be_empty() -> None:
    with pytest.raises(ValidationError):
        LoaderConfig(write_mode='upsert', join_cols=[''])


def test_commit_interval_cannot_be_negative() -> None:
    with pytest.raises(ValidationError):
        LoaderConfig(commit_interval=-1)
