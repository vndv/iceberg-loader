"""Catalog utilities for iceberg-loader.

Provides convenience factories for creating PyIceberg catalog instances
from environment variables.
"""

from __future__ import annotations

import os

from pyiceberg.catalog.rest import RestCatalog


# Public API (exported from iceberg_loader): every option is a keyword argument; bundling them would break callers.
def get_rest_catalog(  # noqa: PLR0913
    name: str = 'rest-catalog',
    *,
    uri: str | None = None,
    warehouse: str | None = None,
    credential: str | None = None,
    s3_endpoint: str | None = None,
    s3_access_key: str | None = None,
    s3_secret_key: str | None = None,
    s3_region: str | None = None,
    s3_path_style: bool = False,
    extra_properties: dict[str, str] | None = None,
) -> RestCatalog:
    """Create a REST Catalog from explicit args or environment variables.

    Args:
        name: Catalog name identifier.
        uri: REST Catalog server endpoint (env: ``ICEBERG_REST_URI``).
        warehouse: Warehouse location, e.g. ``s3://bucket/warehouse/``
                   (env: ``ICEBERG_WAREHOUSE``).
        credential: OAuth2 credential (env: ``ICEBERG_CREDENTIAL``).
        s3_endpoint: S3 endpoint URL (env: ``S3_ENDPOINT``).
        s3_access_key: S3 access key (env: ``S3_ACCESS_KEY``).
        s3_secret_key: S3 secret key (env: ``S3_SECRET_KEY``).
        s3_region: S3 region (env: ``S3_REGION``).
        s3_path_style: Force S3 path-style addressing (``s3.path-style-access``).
                       Defaults to ``False``. Path-style is enabled automatically
                       when ``s3_endpoint`` is set (MinIO-compatible endpoints);
                       set this to ``True`` to force it for other endpoints.
        extra_properties: Additional properties passed directly to ``RestCatalog``.
                          These are applied last and take precedence over every
                          other key, including ``uri``, ``warehouse``,
                          ``credential`` and all ``s3.*`` properties. This is an
                          escape hatch for advanced configuration.

    Returns:
        An initialized ``RestCatalog`` instance.

    Raises:
        ValueError: If ``uri`` is not provided and ``ICEBERG_REST_URI`` is not set.
    """
    resolved_uri = uri or os.environ.get('ICEBERG_REST_URI', '')
    if not resolved_uri:
        raise ValueError(
            'REST Catalog URI is required. Provide the `uri` argument or set ICEBERG_REST_URI.',
        )

    properties: dict[str, str] = {'uri': resolved_uri}

    resolved_warehouse = warehouse or os.environ.get('ICEBERG_WAREHOUSE')
    if resolved_warehouse:
        properties['warehouse'] = resolved_warehouse

    resolved_credential = credential or os.environ.get('ICEBERG_CREDENTIAL')
    if resolved_credential:
        properties['credential'] = resolved_credential

    resolved_s3_endpoint = s3_endpoint or os.environ.get('S3_ENDPOINT')
    if resolved_s3_endpoint:
        properties['s3.endpoint'] = resolved_s3_endpoint

    resolved_s3_access = s3_access_key or os.environ.get('S3_ACCESS_KEY')
    if resolved_s3_access:
        properties['s3.access-key-id'] = resolved_s3_access

    resolved_s3_secret = s3_secret_key or os.environ.get('S3_SECRET_KEY')
    if resolved_s3_secret:
        properties['s3.secret-access-key'] = resolved_s3_secret

    resolved_s3_region = s3_region or os.environ.get('S3_REGION')
    if resolved_s3_region:
        properties['s3.region'] = resolved_s3_region

    if s3_path_style or resolved_s3_endpoint:
        properties['s3.path-style-access'] = 'true'

    if extra_properties:
        properties.update(extra_properties)

    return RestCatalog(name=name, **properties)
