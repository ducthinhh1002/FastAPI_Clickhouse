from fastapi import APIRouter, Depends, HTTPException, Request
from loguru import logger
from app.models.warehouse import Order
from app.services.clickhouse_client import ClickHouseClient


def get_ch(request: Request) -> ClickHouseClient:
    """Lấy đối tượng ClickHouseClient từ ứng dụng."""
    return request.app.state.clickhouse


router = APIRouter(prefix="/orders", tags=["orders"])


@router.post("/")
async def create_order(order: Order, ch: ClickHouseClient = Depends(get_ch)):
    """Tạo mới một đơn hàng."""
    try:
        check_sql = "SELECT count() FROM fact_orders WHERE order_id={order_id:UInt64}"
        exists = ch.query(check_sql, parameters={"order_id": order.order_id}).result_rows[0][0]
        if exists:
            raise HTTPException(status_code=400, detail="Order with this id already exists")

        sql = (
            "INSERT INTO fact_orders (order_id, user_id, product_id, quantity, total) "
            "VALUES ({order_id:UInt64}, {user_id:UInt64}, {product_id:UInt64}, {quantity:UInt32}, {total:Float64})"
        )
        params = {
            "order_id": order.order_id,
            "user_id": order.user_id,
            "product_id": order.product_id,
            "quantity": order.quantity,
            "total": order.total,
        }
        ch.command(sql, parameters=params)
        logger.info("Tạo đơn hàng {}", order.order_id)
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi tạo đơn hàng: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")


@router.get("/{order_id}", response_model=Order)
async def read_order(order_id: int, ch: ClickHouseClient = Depends(get_ch)):
    """Lấy thông tin đơn hàng theo ID."""
    try:
        sql = (
            "SELECT order_id, user_id, product_id, quantity, total, order_date "
            "FROM fact_orders WHERE order_id = {order_id:UInt64}"
        )
        params = {"order_id": order_id}
        result = ch.query(sql, parameters=params)
        rows = result.result_rows
        if not rows:
            raise HTTPException(status_code=404, detail="Order not found")
        r = rows[0]
        return Order(
            order_id=r[0],
            user_id=r[1],
            product_id=r[2],
            quantity=r[3],
            total=r[4],
            order_date=r[5],
        )
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi đọc đơn hàng: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")


@router.put("/{order_id}")
async def update_order(order_id: int, order: Order, ch: ClickHouseClient = Depends(get_ch)):
    """Cập nhật thông tin đơn hàng."""
    try:
        check_sql = "SELECT count() FROM fact_orders WHERE order_id={order_id:UInt64}"
        exists = ch.query(check_sql, parameters={"order_id": order_id}).result_rows[0][0]
        if not exists:
            raise HTTPException(status_code=404, detail="Order not found")

        sql = (
            "ALTER TABLE fact_orders UPDATE user_id={user_id:UInt64}, product_id={product_id:UInt64}, "
            "quantity={quantity:UInt32}, total={total:Float64} WHERE order_id={order_id:UInt64}"
        )
        params = {
            "user_id": order.user_id,
            "product_id": order.product_id,
            "quantity": order.quantity,
            "total": order.total,
            "order_id": order_id,
        }
        ch.command(sql, parameters=params)
        logger.info("Cập nhật đơn hàng {}", order_id)
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi cập nhật đơn hàng: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")


@router.delete("/{order_id}")
async def delete_order(order_id: int, ch: ClickHouseClient = Depends(get_ch)):
    """Xóa đơn hàng theo ID."""
    try:
        check_sql = "SELECT count() FROM fact_orders WHERE order_id={order_id:UInt64}"
        exists = ch.query(check_sql, parameters={"order_id": order_id}).result_rows[0][0]
        if not exists:
            raise HTTPException(status_code=404, detail="Order not found")

        sql = "ALTER TABLE fact_orders DELETE WHERE order_id={order_id:UInt64}"
        params = {"order_id": order_id}
        ch.command(sql, parameters=params)
        logger.info("Xóa đơn hàng {}", order_id)
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi xóa đơn hàng: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")
