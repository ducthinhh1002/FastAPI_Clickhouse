# FastAPI ClickHouse Utilities

## Upload Parquet from MinIO

The `scripts/upload_parquet_minio.py` helper downloads a Parquet file from MinIO and uploads it into ClickHouse in batches, timing the process. Configure MinIO and ClickHouse credentials via command line flags or environment variables. Use `--drop-table` to recreate the table before loading.

The script connects over ClickHouse's HTTP interface. By default this listens on port `8123`, so be sure to specify that port if your server uses the standard configuration.

Example usage:

```bash
python scripts/upload_parquet_minio.py \
  --bucket mybucket \
  --object data.parquet \
  --table parquet_data \
  --batch-size 100000 \
  --ch-port 8123 \
  --drop-table
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

MinIO will be available at `http://localhost:9001` with both user and password set to `minioadmin`. The S3 API listens on `localhost:9002`.
