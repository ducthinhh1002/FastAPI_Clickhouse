from pydantic import BaseModel
from datetime import datetime

class OrderBase(BaseModel):
    customer_id: int
    amount: float

class OrderCreate(OrderBase):
    pass

class Order(OrderBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True
