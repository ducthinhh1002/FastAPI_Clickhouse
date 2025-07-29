# FastAPI ClickHouse Utilities

## Upload Parquet from MinIO

The `scripts/upload_parquet_minio.py` helper downloads a Parquet file from MinIO and uploads it into ClickHouse, timing the process. Configure MinIO and ClickHouse credentials via command line flags or environment variables.

Example usage:

```bash
python scripts/upload_parquet_minio.py \
  --bucket mybucket \
  --object data.parquet \
  --table parquet_data
```

## Generating Test Data

`scripts/generate_parquet.py` creates a Parquet file with random integers for performance testing. By default it generates 10 million rows and 10 columns.

```bash
python scripts/generate_parquet.py --rows 10000000 --cols 10 --output data.parquet
```
