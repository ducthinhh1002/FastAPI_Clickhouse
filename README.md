## Upload Parquet from MinIO

The `scripts/upload_parquet_minio.py` helper downloads a Parquet file from MinIO and uploads it into ClickHouse in batches, timing the process. Configure MinIO and ClickHouse credentials via command line flags or environment variables. Use `--drop-table` to recreate the table before loading.

```bash
python scripts/upload_parquet_minio.py --bucket test --object data10m.parquet --table parquet_data  --batch-size 200000 --drop-table --ch-user admin  --ch-password password
```

## Generating Test Data

`scripts/generate_parquet.py` creates a Parquet file with random integers for performance testing. By default it generates 10 million rows and 10 columns.

```bash
python scripts/generate_parquet.py --rows 10000000 --cols 10 --output data10m.parquet
```

## Check downtime: 
```bash
while ($true) {
    try {
        Invoke-WebRequest -Uri http://localhost:8000/ -UseBasicParsing -ErrorAction Stop > $null
        Write-Host "up"
    } catch {
        Write-Host "down"
    }
    Start-Sleep -Seconds 1
}
```

## CRUD ClickHouse với bảng dim-fact

API hỗ trợ thao tác dữ liệu ClickHouse thông qua các bảng:

- `dim_users`: lưu thông tin người dùng.
- `dim_products`: lưu thông tin sản phẩm.
- `fact_orders`: lưu đơn hàng, tham chiếu tới user và product.

### Ví dụ gọi API

Tạo user mới:

```bash
curl -X POST http://localhost:8000/users/ \
  -H "Content-Type: application/json" \
  -d '{"id":1,"name":"Alice","email":"alice@example.com"}'
```

Tạo product và order tương tự với `/products/` và `/orders/`.

Lấy thông tin đơn hàng:

```bash
curl http://localhost:8000/orders/1
```

### Query tổng hợp từ ClickHouse

Sau khi đã có dữ liệu, có thể truy vấn trực tiếp trong ClickHouse:

```sql
SELECT o.order_id, u.name AS user_name, p.name AS product_name, o.quantity, o.total
FROM fact_orders o
JOIN dim_users u ON o.user_id = u.id
JOIN dim_products p ON o.product_id = p.id;
```

Câu truy vấn trên trả về thông tin đơn hàng kèm tên người dùng và sản phẩm.