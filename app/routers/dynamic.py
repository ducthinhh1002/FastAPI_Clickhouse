from fastapi import APIRouter, Depends, Request, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, Optional
from loguru import logger
from app.services.clickhouse_client import ClickHouseClient


def get_ch(request: Request) -> ClickHouseClient:
    """Lấy đối tượng ClickHouseClient từ ứng dụng.

    Raises:
        HTTPException: Nếu client chưa được khởi tạo.
    """
    try:
        client = request.app.state.clickhouse
        if client is None:
            raise AttributeError("ClickHouse client is None")
        return client
    except AttributeError as exc:
        logger.exception("Không tìm thấy ClickHouseClient: {}", exc)
        raise HTTPException(
            status_code=500, detail="ClickHouse client is not configured"
        )


router = APIRouter(prefix="/sql", tags=["sql"])


class SQLRequest(BaseModel):
    """Dữ liệu yêu cầu thực thi SQL."""

    sql: str
    params: Optional[Dict[str, Any]] = None
    is_select: bool = False


@router.post("/")
async def execute_sql(req: SQLRequest, ch: ClickHouseClient = Depends(get_ch)):
    """Thực thi câu lệnh SQL tùy ý trên ClickHouse."""
    try:
        if req.is_select:
            result = ch.query(req.sql, parameters=req.params or {})
            return {"rows": result.result_rows}
        ch.command(req.sql, parameters=req.params or {})
        return {"status": "ok"}
    except Exception as exc:
        logger.exception("Lỗi thực thi SQL: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")
