from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from typing import Any, Dict, Optional
from app.services.clickhouse_client import ClickHouseClient


def get_ch(request: Request) -> ClickHouseClient:
    return request.app.state.clickhouse


router = APIRouter(prefix="/sql", tags=["sql"])


class SQLRequest(BaseModel):
    sql: str
    params: Optional[Dict[str, Any]] = None
    is_select: bool = False


@router.post("/")
async def execute_sql(req: SQLRequest, ch: ClickHouseClient = Depends(get_ch)):
    """Execute arbitrary SQL against ClickHouse.

    If ``is_select`` is true the rows from the query are returned, otherwise
    the statement is executed as a command.
    """
    if req.is_select:
        result = ch.query(req.sql, parameters=req.params or {})
        return {"rows": result.result_rows}
    ch.command(req.sql, parameters=req.params or {})
    return {"status": "ok"}
