# SPDX-FileCopyrightText: 2025-present Ivan Matveev <skioneim@gmail.com>
#
# SPDX-License-Identifier: MIT

import iceberg_loader.services.logging as logger
from iceberg_loader.__about__ import __version__
from iceberg_loader.catalog import get_rest_catalog
from iceberg_loader.core import (
    IcebergLoader,
    LoaderConfig,
    get_high_watermark,
    load_batches_to_iceberg,
    load_data_to_iceberg,
    load_ipc_stream_to_iceberg,
)
from iceberg_loader.services.maintenance import expire_snapshots

__all__ = [
    'IcebergLoader',
    'LoaderConfig',
    '__version__',
    'expire_snapshots',
    'get_high_watermark',
    'get_rest_catalog',
    'load_batches_to_iceberg',
    'load_data_to_iceberg',
    'load_ipc_stream_to_iceberg',
    'logger',
]
