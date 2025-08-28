"""Định nghĩa các mô hình dữ liệu sử dụng trong ứng dụng."""

from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Union
from pydantic import BaseModel

class User(BaseModel):
    """Thông tin người dùng."""

    id: int
    name: str
    email: str


class Product(BaseModel):
    """Thông tin sản phẩm."""

    id: int
    name: str


class Order(BaseModel):
    """Thông tin đơn hàng."""

    order_id: int
    user_id: int
    product_id: int
    quantity: int
    total: float
    order_date: Optional[datetime] = None

