import argparse
import os
import time
import tempfile

from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq
from clickhouse_connect import get_client


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


def upload_parquet_from_minio(
    endpoint: str,
    access_key: str,
    secret_key: str,
    bucket: str,
    obj: str,
    ch_client,
    dest_table: str,
    batch_size: int = 100000,
    drop_table: bool = False,
) -> int:
    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
    with tempfile.NamedTemporaryFile() as tmp:
        client.fget_object(bucket, obj, tmp.name)
        pq_file = pq.ParquetFile(tmp.name)
        columns = pq_file.schema_arrow.names
        schema = ", ".join(
            f"{name} {arrow_to_clickhouse(pq_file.schema_arrow.field(name).type)}" for name in columns
        )
        if drop_table:
            ch_client.command(f"DROP TABLE IF EXISTS {dest_table}")
        create_sql = f"CREATE TABLE IF NOT EXISTS {dest_table} ({schema}) ENGINE = MergeTree() ORDER BY tuple()"
        ch_client.command(create_sql)

        total_rows = 0
        for batch in pq_file.iter_batches(batch_size=batch_size):
            table = pa.Table.from_batches([batch])
            ch_client.insert_arrow(dest_table, table)
            total_rows += table.num_rows

        return total_rows


def upload_table_to_clickhouse(
    table: pa.Table,
    ch_client,
    dest_table: str,
    batch_size: int = 100000,
    drop_table: bool = False,
):
    columns = table.column_names
    schema = ", ".join(
        f"{name} {arrow_to_clickhouse(table.schema[i].type)}" for i, name in enumerate(columns)
    )
    if drop_table:
        ch_client.command(f"DROP TABLE IF EXISTS {dest_table}")

    create_sql = f"CREATE TABLE IF NOT EXISTS {dest_table} ({schema}) ENGINE = MergeTree() ORDER BY tuple()"
    ch_client.command(create_sql)
    for batch in table.to_batches(batch_size):
        ch_client.insert_arrow(dest_table, pa.Table.from_batches([batch]))


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
    parser.add_argument(
        "--ch-port",
        type=int,
        default=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        help="ClickHouse HTTP port (default 8123)",
    )
    parser.add_argument("--ch-user", default=os.environ.get("CLICKHOUSE_USER", "default"))
    parser.add_argument("--ch-password", default=os.environ.get("CLICKHOUSE_PASSWORD", ""))
    parser.add_argument("--ch-db", default=os.environ.get("CLICKHOUSE_DATABASE", "default"))
    parser.add_argument("--batch-size", type=int, default=100000, help="Rows per insert batch")
    parser.add_argument("--drop-table", action="store_true", help="Drop destination table before loading")


    args = parser.parse_args()

    start = time.time()
    ch_client = get_client(
        host=args.ch_host,
        port=args.ch_port,
        username=args.ch_user,
        password=args.ch_password,
        database=args.ch_db,
    )
    rows = upload_parquet_from_minio(
        endpoint=args.minio_endpoint,
        access_key=args.minio_access,
        secret_key=args.minio_secret,
        bucket=args.bucket,
        obj=args.object,
        ch_client=ch_client,
        dest_table=args.table,
        batch_size=args.batch_size,
        drop_table=args.drop_table,
    )
    elapsed = time.time() - start
    print(f"Uploaded {rows} rows in {elapsed:.2f} seconds")


if __name__ == "__main__":
    main()
