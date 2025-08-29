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

## Simple frontend

Trang HTML đơn giản được phục vụ tại `/frontend` cho phép bạn truy vấn bất kỳ
bảng nào thông qua endpoint CRUD động.

### Hướng dẫn sử dụng
1. Mở trình duyệt truy cập `http://localhost:8000/frontend`.
2. Nhập tên bảng vào ô **Table**.
3. (Tuỳ chọn) Chọn hàm tổng hợp và cột cần tính ở phần **Function** và
   **Aggregate Column**. Có thể nhập cột **Group By** để nhóm kết quả.
4. (Tuỳ chọn) Thêm bộ lọc bằng nút **Add Filter**, mỗi bộ lọc gồm cột và giá trị
   cần so khớp. Có thể thêm nhiều bộ lọc.
5. Nhấn **Fetch** để gửi yêu cầu; kết quả JSON sẽ hiển thị ở khung bên dưới.

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

### Thực thi truy vấn SQL động

API cung cấp endpoint `/sql` để chạy bất kỳ câu lệnh nào tới ClickHouse với
tham số động.  Điều này giúp bạn thao tác với các bảng tự định nghĩa thay vì
chỉ các bảng mẫu trong ví dụ.

Ví dụ thực thi truy vấn `SELECT`:

```bash
Invoke-RestMethod -Uri http://localhost:8000/sql/ `
  -Method POST -ContentType 'application/json' `
  -Body '{"sql":"SELECT count() FROM dim_users WHERE id={id:UInt64}","params":{"id":1},"is_select":true}'
```

Ví dụ thực thi lệnh `INSERT`:

```bash
Invoke-RestMethod -Uri http://localhost:8000/sql/ `
  -Method POST -ContentType 'application/json' `
  -Body '{"sql":"INSERT INTO dim_products (id, name) VALUES ({id:UInt64}, {name:String})","params":{"id":10,"name":"Pen"}}'
```

### CRUD động theo schema

Router `/crud` cho phép thao tác với bất kỳ bảng nào bằng cách tự lấy schema từ
ClickHouse, giúp giảm công viết API cho mỗi bảng mới.

Tạo bản ghi mới:

```bash
Invoke-RestMethod -Uri http://localhost:8000/crud/dim_users `
  -Method POST -ContentType 'application/json' `
  -Body '{"id":5,"name":"User5","email":"user5@example.com"}'
```

Đọc bản ghi:

```bash
Invoke-RestMethod -Uri "http://localhost:8000/crud/dim_users/5" `
  -Method GET
```

Cập nhật bản ghi:

```bash
Invoke-RestMethod -Uri http://localhost:8000/crud/dim_users/5 `
  -Method PUT -ContentType 'application/json' `
  -Body '{"name":"User5 Updated"}'
```

Xóa bản ghi:

```bash
Invoke-RestMethod -Uri "http://localhost:8000/crud/dim_users/5" `
  -Method DELETE
```

Nếu bảng dùng khóa khác `id`, truyền tên cột qua tham số `id_column`:

```bash
Invoke-RestMethod -Uri "http://localhost:8000/crud/fact_orders/1?id_column=order_id" `
  -Method GET
```

