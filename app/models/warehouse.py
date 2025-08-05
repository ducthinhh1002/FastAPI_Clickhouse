from pydantic import BaseModel
from datetime import datetime


class User(BaseModel):
    id: int
    name: str
    email: str


class Product(BaseModel):
    id: int
    name: str


class Order(BaseModel):
    order_id: int
    user_id: int
    product_id: int
    quantity: int
    total: float
    order_date: datetime | None = None
