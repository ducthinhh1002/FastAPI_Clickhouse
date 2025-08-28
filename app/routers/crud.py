from fastapi import APIRouter, Depends, Request, HTTPException
from typing import Any, Dict
from loguru import logger
from app.services.clickhouse_client import ClickHouseClient


def get_ch(request: Request) -> ClickHouseClient:
    """Lấy đối tượng ClickHouseClient từ ứng dụng."""
    return request.app.state.clickhouse


router = APIRouter(prefix="/crud", tags=["crud"])


def _schema_dict(
    ch: ClickHouseClient, table: str
) -> tuple[list[tuple[str, str]], dict[str, str]]:
    """Lấy thông tin cột và xây dựng từ điển schema."""
    try:
        columns = ch.get_table_schema(table)
        schema = {name: dtype for name, dtype in columns}
        return columns, schema
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi lấy schema bảng {}: {}", table, exc)
        message = str(exc).lower()
        if "doesn't exist" in message or "unknown table" in message:
            raise HTTPException(status_code=404, detail="Bảng không tồn tại")
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")


def _cast_value(value: str, ch_type: str) -> Any:
    """Chuyển đổi giá trị từ chuỗi theo kiểu dữ liệu ClickHouse."""
    try:
        if ch_type.startswith("UInt") or ch_type.startswith("Int"):
            return int(value)
        if ch_type.startswith("Float"):
            return float(value)
        return value
    except Exception as exc:
        logger.warning("Không thể ép kiểu giá trị {} sang {}: {}", value, ch_type, exc)
        return value


@router.post("/{table}")
async def create_row(table: str, data: Dict[str, Any], ch: ClickHouseClient = Depends(get_ch)):
    """Chèn bản ghi mới vào bảng bất kỳ."""
    try:
        columns, schema = _schema_dict(ch, table)
        unknown = set(data) - set(schema)
        if unknown:
            raise HTTPException(status_code=400, detail=f"Unknown columns: {', '.join(unknown)}")
        cols = list(data.keys())
        cols_str = ", ".join(cols)
        placeholders = ", ".join([f"{{{c}:{schema[c]}}}" for c in cols])
        sql = f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders})"
        ch.command(sql, parameters=data)
        logger.info("Chèn dữ liệu vào bảng {}", table)
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi chèn dữ liệu bảng {}: {}", table, exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")


@router.get("/{table}")
async def query_rows(table: str, request: Request, ch: ClickHouseClient = Depends(get_ch)):
    """Truy vấn các bản ghi với bộ lọc linh hoạt."""
    try:
        columns, schema = _schema_dict(ch, table)
        filters: Dict[str, Any] = {}
        for key, value in request.query_params.items():
            if key not in schema:
                raise HTTPException(status_code=400, detail=f"Invalid filter column: {key}")
            filters[key] = _cast_value(value, schema[key])
        sql = f"SELECT * FROM {table}"
        if filters:
            conditions = " AND ".join([f"{k}={{{k}:{schema[k]}}}" for k in filters])
            sql += f" WHERE {conditions}"
        result = ch.query(sql, parameters=filters)
        return [
            {col: row[idx] for idx, (col, _) in enumerate(columns)}
            for row in result.result_rows
        ]
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi truy vấn bảng {}: {}", table, exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")


@router.get("/{table}/{item_id}")
async def read_row(table: str, item_id: str, id_column: str = "id", ch: ClickHouseClient = Depends(get_ch)):
    """Đọc một bản ghi theo khóa chính."""
    try:
        columns, schema = _schema_dict(ch, table)
        if id_column not in schema:
            raise HTTPException(status_code=400, detail="Invalid id column")
        id_value = _cast_value(item_id, schema[id_column])
        sql = f"SELECT * FROM {table} WHERE {id_column}={{{id_column}:{schema[id_column]}}}"
        result = ch.query(sql, parameters={id_column: id_value})
        rows = result.result_rows
        if not rows:
            raise HTTPException(status_code=404, detail="Row not found")
        row = rows[0]
        return {col: row[idx] for idx, (col, _) in enumerate(columns)}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi đọc dữ liệu bảng {}: {}", table, exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")


@router.put("/{table}/{item_id}")
async def update_row(table: str, item_id: str, data: Dict[str, Any], id_column: str = "id", ch: ClickHouseClient = Depends(get_ch)):
    """Cập nhật bản ghi theo khóa chính."""
    try:
        _, schema = _schema_dict(ch, table)
        if id_column not in schema:
            raise HTTPException(status_code=400, detail="Invalid id column")
        unknown = set(data) - set(schema)
        if unknown:
            raise HTTPException(status_code=400, detail=f"Unknown columns: {', '.join(unknown)}")
        set_clause = ", ".join([f"{c}={{{c}:{schema[c]}}}" for c in data.keys()])
        sql = f"ALTER TABLE {table} UPDATE {set_clause} WHERE {id_column}={{{id_column}:{schema[id_column]}}}"
        params = {**data, id_column: _cast_value(item_id, schema[id_column])}
        ch.command(sql, parameters=params)
        logger.info("Cập nhật dữ liệu bảng {}", table)
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi cập nhật dữ liệu bảng {}: {}", table, exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")


@router.delete("/{table}/{item_id}")
async def delete_row(table: str, item_id: str, id_column: str = "id", ch: ClickHouseClient = Depends(get_ch)):
    """Xóa bản ghi theo khóa chính."""
    try:
        _, schema = _schema_dict(ch, table)
        if id_column not in schema:
            raise HTTPException(status_code=400, detail="Invalid id column")
        sql = f"ALTER TABLE {table} DELETE WHERE {id_column}={{{id_column}:{schema[id_column]}}}"
        ch.command(sql, parameters={id_column: _cast_value(item_id, schema[id_column])})
        logger.info("Xóa dữ liệu bảng {}", table)
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi xóa dữ liệu bảng {}: {}", table, exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")
