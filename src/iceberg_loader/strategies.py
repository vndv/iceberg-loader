import logging
from abc import ABC, abstractmethod
from typing import Any

import pyarrow as pa

logger = logging.getLogger(__name__)


class WriteStrategy(ABC):
    """
    Abstract base class for write strategies.
    Defines how data should be written to the Iceberg table.
    """

    @abstractmethod
    def write(self, table: Any, data: pa.Table, is_first_write: bool) -> None:
        """
        Write data to the table.

        Args:
            table: The Iceberg table object.
            data: The PyArrow table to write.
            is_first_write: True if this is the first batch/transaction in the stream.
        """
        pass


class AppendStrategy(WriteStrategy):
    """Simply appends data to the table."""

    def write(self, table: Any, data: pa.Table, is_first_write: bool) -> None:
        with table.transaction() as txn:
            txn.append(data)


class OverwriteStrategy(WriteStrategy):
    """
    Overwrites the table content on the first write,
    then appends for subsequent batches (to preserve the stream data).
    """

    def write(self, table: Any, data: pa.Table, is_first_write: bool) -> None:
        with table.transaction() as txn:
            if is_first_write:
                logger.info('First write in Overwrite mode: Overwriting table data.')
                txn.overwrite(data)
            else:
                txn.append(data)


class IdempotentStrategy(WriteStrategy):
    """
    Deletes rows matching a filter before writing (on first write),
    then appends data. Implements 'Replace Partition' pattern.
    """

    def __init__(self, replace_filter: str):
        self.replace_filter = replace_filter

    def write(self, table: Any, data: pa.Table, is_first_write: bool) -> None:
        with table.transaction() as txn:
            if is_first_write:
                logger.info("First write in Idempotent mode: Deleting rows matching '%s'", self.replace_filter)
                txn.delete(self.replace_filter)

            txn.append(data)


class UpsertStrategy(WriteStrategy):
    """
    Upserts data into the table.
    Uses PyIceberg's upsert functionality (Merge Into).
    """

    def __init__(self, join_cols: list[str] | None = None):
        self.join_cols = join_cols

    def write(self, table: Any, data: pa.Table, is_first_write: bool) -> None:
        # Upsert handles its own transaction logic internally in PyIceberg
        logger.debug('Upserting data...')
        table.upsert(df=data, join_cols=self.join_cols)


def get_write_strategy(
    write_mode: str,
    replace_filter: str | None = None,
    join_cols: list[str] | None = None,
) -> WriteStrategy:
    """Factory method to select the appropriate strategy."""
    if write_mode == 'upsert':
        return UpsertStrategy(join_cols=join_cols)

    if replace_filter:
        return IdempotentStrategy(replace_filter)

    if write_mode == 'overwrite':
        return OverwriteStrategy()

    return AppendStrategy()
