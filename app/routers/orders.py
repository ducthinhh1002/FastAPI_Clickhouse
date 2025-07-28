from fastapi import APIRouter, Depends, HTTPException, Request
from typing import List
from app.models.order import Order, OrderCreate
from app.core.security import get_current_user

router = APIRouter(prefix="/orders", tags=["orders"])

@router.post("/", response_model=Order)
async def create_order(
    request: Request,
    order: OrderCreate,
    current_user: str = Depends(get_current_user),
):
    client = request.app.state.clickhouse
    client.insert_order(order.customer_id, order.amount)
    order_id = client._get_next_id('fact_order') - 1
    result = client.get_order(order_id)
    if not result:
        raise HTTPException(status_code=404, detail="Order not found")
    return {
        "id": result[0],
        "customer_id": result[1],
        "amount": result[2],
        "created_at": result[3],
    }

@router.get("/", response_model=List[Order])
async def read_orders(
    request: Request,
    skip: int = 0,
    limit: int = 10,
    current_user: str = Depends(get_current_user),
):
    client = request.app.state.clickhouse
    results = client.get_orders(skip, limit)
    return [
        {
            "id": r[0],
            "customer_id": r[1],
            "amount": r[2],
            "created_at": r[3],
        }
        for r in results
    ]

@router.get("/{order_id}", response_model=Order)
async def read_order(
    request: Request,
    order_id: int,
    current_user: str = Depends(get_current_user),
):
    client = request.app.state.clickhouse
    result = client.get_order(order_id)
    if not result:
        raise HTTPException(status_code=404, detail="Order not found")
    return {
        "id": result[0],
        "customer_id": result[1],
        "amount": result[2],
        "created_at": result[3],
    }

@router.put("/{order_id}", response_model=Order)
async def update_order(
    request: Request,
    order_id: int,
    order: OrderCreate,
    current_user: str = Depends(get_current_user),
):
    client = request.app.state.clickhouse
    client.update_order(order_id, order.customer_id, order.amount)
    result = client.get_order(order_id)
    if not result:
        raise HTTPException(status_code=404, detail="Order not found")
    return {
        "id": result[0],
        "customer_id": result[1],
        "amount": result[2],
        "created_at": result[3],
    }

@router.delete("/{order_id}")
async def delete_order(
    request: Request,
    order_id: int,
    current_user: str = Depends(get_current_user),
):
    client = request.app.state.clickhouse
    result = client.delete_order(order_id)
    if not result:
        raise HTTPException(status_code=404, detail="Order not found")
    return {"message": "Order deleted"}
