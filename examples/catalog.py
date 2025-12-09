import logging

from pyiceberg.catalog.hive import HiveCatalog

from settings import settings

logger = logging.getLogger(__name__)


def get_catalog():
    """Initialize and return Iceberg catalog with S3 configuration."""
    s3_properties = {
        's3.endpoint': settings.S3_ENDPOINT,
        's3.access-key-id': settings.S3_ACCESS_KEY,
        's3.secret-access-key': settings.S3_SECRET_KEY,
        's3.path-style-access': 'true',
        's3.region': settings.S3_REGION,
        'py-io-impl': 'pyiceberg.io.fsspec.FsspecFileIO',
    }

    logger.info('Initializing Iceberg catalog: %s', settings.HIVE_URI)
    
    return HiveCatalog(
        name='default',
        uri=settings.HIVE_URI,
        warehouse=settings.S3_BUCKET,
        **s3_properties,
    )
