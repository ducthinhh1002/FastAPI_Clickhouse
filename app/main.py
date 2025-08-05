from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.routers import auth, users, products, orders
from app.services.clickhouse_client import ClickHouseClient

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.clickhouse = ClickHouseClient()
    app.state.clickhouse.init_db()
    yield

app = FastAPI(title="FastAPI ClickHouse API", lifespan=lifespan)

app.include_router(auth.router)
app.include_router(users.router)
app.include_router(products.router)
app.include_router(orders.router)

@app.get("/")
async def root():
    return {"message": "FastAPI ClickHouse API"}
