from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.routers import auth
from app.services.clickhouse_client import ClickHouseClient

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.clickhouse = ClickHouseClient()
    yield

app = FastAPI(title="FastAPI ClickHouse API", lifespan=lifespan)

app.include_router(auth.router)

@app.get("/")
async def root():
    return {"message": "FastAPI ClickHouse API"}
