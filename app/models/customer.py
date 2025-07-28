from pydantic import BaseModel

class CustomerBase(BaseModel):
    name: str

class CustomerCreate(CustomerBase):
    pass

class Customer(CustomerBase):
    id: int

    class Config:
        from_attributes = True
