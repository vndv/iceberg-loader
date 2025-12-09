import os
from pathlib import Path


class Settings:
    """Settings for examples - uses environment variables with defaults for local setup."""
    
    ENV: str = os.getenv('ENV', 'dev')
    HIVE_URI: str = os.getenv('HIVE_URI', 'thrift://localhost:9083')
    S3_ENDPOINT: str = os.getenv('S3_ENDPOINT', 'http://localhost:9000')
    S3_ACCESS_KEY: str = os.getenv('S3_ACCESS_KEY', 'minio')
    S3_SECRET_KEY: str = os.getenv('S3_SECRET_KEY', 'minio123')
    S3_BUCKET: str = os.getenv('S3_BUCKET', 's3://datalake/warehouse')
    S3_REGION: str = os.getenv('S3_REGION', 'us-east-1')


settings = Settings()
