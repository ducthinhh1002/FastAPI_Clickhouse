from fastapi import APIRouter, Depends, HTTPException, Request
from typing import List
from app.models.customer import Customer, CustomerCreate
from app.core.security import get_current_user

router = APIRouter(prefix="/customers", tags=["customers"])

@router.post("/", response_model=Customer)
async def create_customer(
    request: Request,
    customer: CustomerCreate,
    current_user: str = Depends(get_current_user),
):
    client = request.app.state.clickhouse
    client.insert_customer(customer.name)
    customer_id = client._get_next_id('dim_customer') - 1
    result = client.get_customer(customer_id)
    if not result:
        raise HTTPException(status_code=404, detail="Customer not found")
    return {"id": result[0], "name": result[1]}

@router.get("/", response_model=List[Customer])
async def read_customers(
    request: Request,
    skip: int = 0,
    limit: int = 10,
    current_user: str = Depends(get_current_user),
):
    client = request.app.state.clickhouse
    results = client.get_customers(skip, limit)
    return [
        {"id": r[0], "name": r[1]} for r in results
    ]

@router.get("/{customer_id}", response_model=Customer)
async def read_customer(
    request: Request,
    customer_id: int,
    current_user: str = Depends(get_current_user),
):
    client = request.app.state.clickhouse
    result = client.get_customer(customer_id)
    if not result:
        raise HTTPException(status_code=404, detail="Customer not found")
    return {"id": result[0], "name": result[1]}

@router.put("/{customer_id}", response_model=Customer)
async def update_customer(
    request: Request,
    customer_id: int,
    customer: CustomerCreate,
    current_user: str = Depends(get_current_user),
):
    client = request.app.state.clickhouse
    client.update_customer(customer_id, customer.name)
    result = client.get_customer(customer_id)
    if not result:
        raise HTTPException(status_code=404, detail="Customer not found")
    return {"id": result[0], "name": result[1]}

@router.delete("/{customer_id}")
async def delete_customer(
    request: Request,
    customer_id: int,
    current_user: str = Depends(get_current_user),
):
    client = request.app.state.clickhouse
    result = client.delete_customer(customer_id)
    if not result:
        raise HTTPException(status_code=404, detail="Customer not found")
    return {"message": "Customer deleted"}
