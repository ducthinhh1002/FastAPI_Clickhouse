from fastapi import APIRouter, Depends, HTTPException, Request
from loguru import logger
from app.models.warehouse import Product
from app.services.clickhouse_client import ClickHouseClient


def get_ch(request: Request) -> ClickHouseClient:
    """Lấy đối tượng ClickHouseClient từ ứng dụng.

    Raises:
        HTTPException: Nếu client chưa được khởi tạo.
    """
    try:
        client = request.app.state.clickhouse
        if client is None:
            raise AttributeError("ClickHouse client is None")
        return client
    except AttributeError as exc:
        logger.exception("Không tìm thấy ClickHouseClient: {}", exc)
        raise HTTPException(
            status_code=500, detail="ClickHouse client is not configured"
        )


router = APIRouter(prefix="/products", tags=["products"])


@router.post("/")
async def create_product(product: Product, ch: ClickHouseClient = Depends(get_ch)):
    """Tạo mới một sản phẩm."""
    try:
        check_sql = "SELECT count() FROM dim_products WHERE id={id:UInt64}"
        exists = ch.query(check_sql, parameters={"id": product.id}).result_rows[0][0]
        if exists:
            raise HTTPException(status_code=400, detail="Product with this id already exists")

        sql = "INSERT INTO dim_products (id, name) VALUES ({id:UInt64}, {name:String})"
        params = {"id": product.id, "name": product.name}
        ch.command(sql, parameters=params)
        logger.info("Tạo sản phẩm {}", product.id)
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi tạo sản phẩm: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")


@router.get("/{product_id}", response_model=Product)
async def read_product(product_id: int, ch: ClickHouseClient = Depends(get_ch)):
    """Lấy thông tin sản phẩm theo ID."""
    try:
        sql = "SELECT id, name FROM dim_products WHERE id = {product_id:UInt64}"
        params = {"product_id": product_id}
        result = ch.query(sql, parameters=params)
        rows = result.result_rows
        if not rows:
            raise HTTPException(status_code=404, detail="Product not found")
        r = rows[0]
        return Product(id=r[0], name=r[1])
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi đọc sản phẩm: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")


@router.put("/{product_id}")
async def update_product(product_id: int, product: Product, ch: ClickHouseClient = Depends(get_ch)):
    """Cập nhật thông tin sản phẩm."""
    try:
        check_sql = "SELECT count() FROM dim_products WHERE id={product_id:UInt64}"
        exists = ch.query(check_sql, parameters={"product_id": product_id}).result_rows[0][0]
        if not exists:
            raise HTTPException(status_code=404, detail="Product not found")

        sql = "ALTER TABLE dim_products UPDATE name={name:String} WHERE id={product_id:UInt64}"
        params = {"name": product.name, "product_id": product_id}
        ch.command(sql, parameters=params)
        logger.info("Cập nhật sản phẩm {}", product_id)
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi cập nhật sản phẩm: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")


@router.delete("/{product_id}")
async def delete_product(product_id: int, ch: ClickHouseClient = Depends(get_ch)):
    """Xóa sản phẩm theo ID."""
    try:
        check_sql = "SELECT count() FROM dim_products WHERE id={product_id:UInt64}"
        exists = ch.query(check_sql, parameters={"product_id": product_id}).result_rows[0][0]
        if not exists:
            raise HTTPException(status_code=404, detail="Product not found")

        sql = "ALTER TABLE dim_products DELETE WHERE id={product_id:UInt64}"
        params = {"product_id": product_id}
        ch.command(sql, parameters=params)
        logger.info("Xóa sản phẩm {}", product_id)
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi xóa sản phẩm: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")
