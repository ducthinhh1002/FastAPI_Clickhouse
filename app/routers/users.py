from fastapi import APIRouter, Depends, HTTPException, Request
from loguru import logger
from app.models.warehouse import User
from app.services.clickhouse_client import ClickHouseClient


def get_ch(request: Request) -> ClickHouseClient:
    """Lấy đối tượng ClickHouseClient từ ứng dụng."""
    return request.app.state.clickhouse


router = APIRouter(prefix="/users", tags=["users"])


@router.post("/")
async def create_user(user: User, ch: ClickHouseClient = Depends(get_ch)):
    """Tạo mới một người dùng."""
    try:
        check_sql = "SELECT count() FROM dim_users WHERE id={id:UInt64}"
        exists = ch.query(check_sql, parameters={"id": user.id}).result_rows[0][0]
        if exists:
            raise HTTPException(status_code=400, detail="User with this id already exists")

        sql = "INSERT INTO dim_users (id, name, email) VALUES ({id:UInt64}, {name:String}, {email:String})"
        params = {"id": user.id, "name": user.name, "email": user.email}
        ch.command(sql, parameters=params)
        logger.info("Tạo người dùng {}", user.id)
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi tạo người dùng: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")


@router.get("/{user_id}", response_model=User)
async def read_user(user_id: int, ch: ClickHouseClient = Depends(get_ch)):
    """Lấy thông tin một người dùng theo ID."""
    try:
        sql = "SELECT id, name, email FROM dim_users WHERE id = {user_id:UInt64}"
        params = {"user_id": user_id}
        result = ch.query(sql, parameters=params)
        rows = result.result_rows
        if not rows:
            raise HTTPException(status_code=404, detail="User not found")
        r = rows[0]
        return User(id=r[0], name=r[1], email=r[2])
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi đọc người dùng: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")


@router.put("/{user_id}")
async def update_user(user_id: int, user: User, ch: ClickHouseClient = Depends(get_ch)):
    """Cập nhật thông tin người dùng."""
    try:
        check_sql = "SELECT count() FROM dim_users WHERE id={user_id:UInt64}"
        exists = ch.query(check_sql, parameters={"user_id": user_id}).result_rows[0][0]
        if not exists:
            raise HTTPException(status_code=404, detail="User not found")

        sql = "ALTER TABLE dim_users UPDATE name={name:String}, email={email:String} WHERE id={user_id:UInt64}"
        params = {"name": user.name, "email": user.email, "user_id": user_id}
        ch.command(sql, parameters=params)
        logger.info("Cập nhật người dùng {}", user_id)
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi cập nhật người dùng: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")


@router.delete("/{user_id}")
async def delete_user(user_id: int, ch: ClickHouseClient = Depends(get_ch)):
    """Xóa người dùng theo ID."""
    try:
        check_sql = "SELECT count() FROM dim_users WHERE id={user_id:UInt64}"
        exists = ch.query(check_sql, parameters={"user_id": user_id}).result_rows[0][0]
        if not exists:
            raise HTTPException(status_code=404, detail="User not found")

        sql = "ALTER TABLE dim_users DELETE WHERE id={user_id:UInt64}"
        params = {"user_id": user_id}
        ch.command(sql, parameters=params)
        logger.info("Xóa người dùng {}", user_id)
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi xóa người dùng: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")
