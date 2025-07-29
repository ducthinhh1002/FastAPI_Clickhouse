import argparse
import io
import os
import time

from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq
from clickhouse_driver import Client


def arrow_to_clickhouse(pa_type: pa.DataType) -> str:
    if pa.types.is_integer(pa_type):
        return "Int64"
    if pa.types.is_floating(pa_type):
        return "Float64"
    if pa.types.is_boolean(pa_type):
        return "UInt8"
    if pa.types.is_timestamp(pa_type):
        return "DateTime"
    return "String"


def read_parquet_from_minio(
    endpoint: str,
    access_key: str,
    secret_key: str,
    bucket: str,
    obj: str,
) -> pa.Table:
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
    response = client.get_object(bucket, obj)
    try:
        data = response.read()
    finally:
        response.close()
        response.release_conn()
    buffer = io.BytesIO(data)
    return pq.read_table(buffer)


def upload_table_to_clickhouse(
    table: pa.Table,
    ch_client: Client,
    dest_table: str,
    batch_size: int = 100000,
):
    columns = table.column_names
    schema = ", ".join(
        f"{name} {arrow_to_clickhouse(table.schema[i].type)}" for i, name in enumerate(columns)
    )
    create_sql = f"CREATE TABLE IF NOT EXISTS {dest_table} ({schema}) ENGINE = MergeTree() ORDER BY tuple()"
    ch_client.execute(create_sql)
    insert_sql = f"INSERT INTO {dest_table} ({', '.join(columns)}) VALUES"
    for batch in table.to_batches(batch_size):
        rows = list(zip(*[batch.column(i).to_pylist() for i in range(batch.num_columns)]))
        ch_client.execute(insert_sql, rows)


def main():
    parser = argparse.ArgumentParser(description="Upload parquet file from MinIO to ClickHouse")
    parser.add_argument("--bucket", required=True, help="MinIO bucket name")
    parser.add_argument("--object", required=True, help="Parquet object name")
    parser.add_argument("--table", required=True, help="Destination ClickHouse table")
    parser.add_argument(
        "--minio-endpoint",
        default=os.environ.get("MINIO_ENDPOINT", "localhost:9002"),
    )
    parser.add_argument("--minio-access", default=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"))
    parser.add_argument("--minio-secret", default=os.environ.get("MINIO_SECRET_KEY", "minioadmin"))
    parser.add_argument("--ch-host", default=os.environ.get("CLICKHOUSE_HOST", "localhost"))
    parser.add_argument("--ch-port", type=int, default=int(os.environ.get("CLICKHOUSE_PORT", 9000)))
    parser.add_argument("--ch-user", default=os.environ.get("CLICKHOUSE_USER", "default"))
    parser.add_argument("--ch-password", default=os.environ.get("CLICKHOUSE_PASSWORD", ""))
    parser.add_argument("--ch-db", default=os.environ.get("CLICKHOUSE_DATABASE", "default"))
    parser.add_argument("--batch-size", type=int, default=100000, help="Rows per insert batch")

    args = parser.parse_args()

    start = time.time()
    table = read_parquet_from_minio(
        endpoint=args.minio_endpoint,
        access_key=args.minio_access,
        secret_key=args.minio_secret,
        bucket=args.bucket,
        obj=args.object,
    )

    ch_client = Client(
        host=args.ch_host,
        port=args.ch_port,
        user=args.ch_user,
        password=args.ch_password,
        database=args.ch_db,
    )
    upload_table_to_clickhouse(table, ch_client, args.table, batch_size=args.batch_size)
    elapsed = time.time() - start
    print(f"Uploaded {table.num_rows} rows in {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()
