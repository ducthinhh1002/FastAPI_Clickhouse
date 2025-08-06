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
Invoke-RestMethod -Uri http://localhost:8000/users/ `
  -Method Post -ContentType 'application/json' `
  -Body '{"id":2,"name":"B","email":"alice@example.com"}'
```
Lấy thông tin user:
```bash
Invoke-RestMethod -Uri "http://localhost:8000/users/1" `
  -Method GET `
  -ContentType "application/json"
```
Cập nhật user:
```bash
Invoke-RestMethod -Uri http://localhost:8000/users/1 `
  -Method PUT -ContentType 'application/json' `
  -Body '{"id":1,"name":"Alice Updated","email":"alice_new@example.com"}'
```
Xóa user:
```bash
Invoke-RestMethod -Uri "http://localhost:8000/users/1" `
  -Method DELETE `
  -ContentType "application/json"
```
Tạo product mới:
```bash
Invoke-RestMethod -Uri http://localhost:8000/products/ `
  -Method POST `
  -ContentType "application/json" `
  -Body '{"id":3,"name":"Book"}'
```
Lấy thông tin product:
```bash
Invoke-RestMethod -Uri "http://localhost:8000/products/3" `
  -Method GET `
  -ContentType "application/json"
```
Cập nhật product:
```bash
Invoke-RestMethod -Uri http://localhost:8000/products/3 `
  -Method PUT -ContentType 'application/json' `
  -Body '{"id":3,"name":"Book Updated"}'
```
Xóa product:
```bash
Invoke-RestMethod -Uri "http://localhost:8000/products/3" `
  -Method DELETE `
  -ContentType "application/json"
```
Tạo đơn hàng mới:
```bash
Invoke-RestMethod -Uri http://localhost:8000/orders/ `
  -Method POST `
  -ContentType "application/json" `
  -Body '{"order_id":3,"user_id":2,"product_id":3,"quantity":3,"total":80}'
```
Lấy thông tin đơn hàng:
```bash
Invoke-RestMethod -Uri "http://localhost:8000/orders/1" `
  -Method GET `
  -ContentType "application/json"
```
Cập nhật đơn hàng:
```bash
Invoke-RestMethod -Uri http://localhost:8000/orders/1 `
  -Method PUT -ContentType 'application/json' `
  -Body '{"order_id":1,"user_id":1,"product_id":3,"quantity":10,"total":80}'
```
Xóa đơn hàng:
```bash
Invoke-RestMethod -Uri "http://localhost:8000/orders/1" `
  -Method DELETE `
  -ContentType "application/json"
```
### Query tổng hợp từ ClickHouse

Sau khi đã có dữ liệu, có thể truy vấn trực tiếp trong ClickHouse:

```sql
SELECT o.order_id, u.name, p.name, o.quantity, o.total
FROM fact_orders o
LEFT JOIN dim_users u ON o.user_id = u.id
LEFT JOIN dim_products p ON o.product_id = p.id;
```

