from fastapi import FastAPI, Depends
from fastapi.security import OAuth2PasswordBearer
from contextlib import asynccontextmanager
from app.routers import auth, customers, orders
from app.core.config import settings
from app.services.clickhouse_client import ClickHouseClient

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.clickhouse = ClickHouseClient()
    yield

app = FastAPI(title="FastAPI ClickHouse API", lifespan=lifespan)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")

app.include_router(auth.router)
app.include_router(customers.router)
app.include_router(orders.router)

@app.get("/")
async def root():
    return {"message": "FastAPI ClickHouse API"}
