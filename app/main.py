from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager
from loguru import logger
from pathlib import Path
from app.routers import auth, users, products, orders, dynamic, crud
from app.services.clickhouse_client import ClickHouseClient

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Quản lý vòng đời ứng dụng và kết nối ClickHouse."""
    app.state.clickhouse = ClickHouseClient()
    app.state.clickhouse.init_db()
    logger.info("Ứng dụng khởi động")
    try:
        yield
    finally:
        logger.info("Ứng dụng dừng")

BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="FastAPI ClickHouse API", lifespan=lifespan)

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

app.include_router(auth.router)
app.include_router(users.router)
app.include_router(products.router)
app.include_router(orders.router)
app.include_router(dynamic.router)
app.include_router(crud.router)


@app.get("/frontend")
async def frontend() -> FileResponse:
    """Render simple frontend page."""
    return FileResponse(BASE_DIR / "static" / "index.html")

@app.get("/")
async def root():
    """API kiểm tra trạng thái."""
    return {"message": "FastAPI ClickHouse API"}
