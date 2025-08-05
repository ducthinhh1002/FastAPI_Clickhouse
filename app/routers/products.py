from fastapi import APIRouter, Depends, HTTPException, Request
from app.models.warehouse import Product
from app.services.clickhouse_client import ClickHouseClient


def get_ch(request: Request) -> ClickHouseClient:
    return request.app.state.clickhouse


router = APIRouter(prefix="/products", tags=["products"])


@router.post("/")
async def create_product(product: Product, ch: ClickHouseClient = Depends(get_ch)):
    ch.command(f"INSERT INTO dim_products (id, name) VALUES ({product.id}, '{product.name}')")
    return {"status": "ok"}


@router.get("/{product_id}", response_model=Product)
async def read_product(product_id: int, ch: ClickHouseClient = Depends(get_ch)):
    result = ch.query(f"SELECT id, name FROM dim_products WHERE id = {product_id}")
    rows = result.result_rows
    if not rows:
        raise HTTPException(status_code=404, detail="Product not found")
    r = rows[0]
    return Product(id=r[0], name=r[1])


@router.put("/{product_id}")
async def update_product(product_id: int, product: Product, ch: ClickHouseClient = Depends(get_ch)):
    ch.command(f"ALTER TABLE dim_products UPDATE name='{product.name}' WHERE id={product_id}")
    return {"status": "ok"}


@router.delete("/{product_id}")
async def delete_product(product_id: int, ch: ClickHouseClient = Depends(get_ch)):
    ch.command(f"ALTER TABLE dim_products DELETE WHERE id={product_id}")
    return {"status": "ok"}
