# FastAPI ClickHouse Utilities

## Upload Parquet from MinIO

The `scripts/upload_parquet_minio.py` helper downloads a Parquet file from MinIO and uploads it into ClickHouse in batches, timing the process. Configure MinIO and ClickHouse credentials via command line flags or environment variables.

Example usage:

```bash
python scripts/upload_parquet_minio.py \
  --bucket mybucket \
  --object data.parquet \
  --table parquet_data \
  --batch-size 100000
```

## Generating Test Data

`scripts/generate_parquet.py` creates a Parquet file with random integers for performance testing. By default it generates 10 million rows and 10 columns.

```bash
python scripts/generate_parquet.py --rows 10000000 --cols 10 --output data.parquet
```

## Running with Docker

The included `docker-compose.yml` spins up ClickHouse, MinIO and the FastAPI app. Make sure Docker is installed and the daemon is running, then start the services:

```bash
docker compose up --build -d
```

MinIO will be available at `http://localhost:9001` with both user and password set to `minioadmin`.
