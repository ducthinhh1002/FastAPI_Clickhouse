from fastapi import APIRouter, Depends, Request, HTTPException
from pydantic import BaseModel
from typing import Any, Dict, Optional
from loguru import logger
from app.services.clickhouse_client import ClickHouseClient


def get_ch(request: Request) -> ClickHouseClient:
    """Lấy đối tượng ClickHouseClient từ ứng dụng."""
    return request.app.state.clickhouse


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
