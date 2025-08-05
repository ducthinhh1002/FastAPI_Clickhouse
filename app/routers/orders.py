from fastapi import APIRouter, Depends, HTTPException, Request
from app.models.warehouse import Order
from app.services.clickhouse_client import ClickHouseClient


def get_ch(request: Request) -> ClickHouseClient:
    return request.app.state.clickhouse


router = APIRouter(prefix="/orders", tags=["orders"])


@router.post("/")
async def create_order(order: Order, ch: ClickHouseClient = Depends(get_ch)):
    ch.command(
        f"INSERT INTO fact_orders (order_id, user_id, product_id, quantity, total) VALUES ({order.order_id}, {order.user_id}, {order.product_id}, {order.quantity}, {order.total})"
    )
    return {"status": "ok"}


@router.get("/{order_id}", response_model=Order)
async def read_order(order_id: int, ch: ClickHouseClient = Depends(get_ch)):
    result = ch.query(
        f"SELECT order_id, user_id, product_id, quantity, total, order_date FROM fact_orders WHERE order_id = {order_id}"
    )
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


@router.put("/{order_id}")
async def update_order(order_id: int, order: Order, ch: ClickHouseClient = Depends(get_ch)):
    ch.command(
        f"ALTER TABLE fact_orders UPDATE user_id={order.user_id}, product_id={order.product_id}, quantity={order.quantity}, total={order.total} WHERE order_id={order_id}"
    )
    return {"status": "ok"}


@router.delete("/{order_id}")
async def delete_order(order_id: int, ch: ClickHouseClient = Depends(get_ch)):
    ch.command(f"ALTER TABLE fact_orders DELETE WHERE order_id={order_id}")
    return {"status": "ok"}
