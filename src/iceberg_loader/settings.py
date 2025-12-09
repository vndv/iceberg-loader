TABLE_PROPERTIES = {
    'write.format.default': 'parquet',
    'format-version': 2,
    'write.parquet.compression-codec': 'zstd',
    'commit.retry.num-retries': 10,
    'commit.retry.min-wait-ms': 100,
    'commit.retry.max-wait-ms': 60000,
}
