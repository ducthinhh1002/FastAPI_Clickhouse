from fastapi import APIRouter, HTTPException, status, Depends, Request
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from loguru import logger
import hashlib

from app.core.security import create_access_token
from app.services.clickhouse_client import ClickHouseClient

router = APIRouter(prefix="/auth", tags=["auth"])


def get_ch(request: Request) -> ClickHouseClient:
    """Lấy đối tượng ClickHouseClient từ ứng dụng."""
    return request.app.state.clickhouse


class RegisterForm(BaseModel):
    """Model dữ liệu cho việc đăng ký người dùng."""

    username: str
    password: str


@router.post("/register")
async def register(user: RegisterForm, ch: ClickHouseClient = Depends(get_ch)):
    """Đăng ký tài khoản mới và trả về token truy cập."""
    try:
        check_sql = "SELECT count() FROM users_auth WHERE username={username:String}"
        exists = ch.query(check_sql, parameters={"username": user.username}).result_rows[0][0]
        if exists:
            raise HTTPException(status_code=400, detail="Username already registered")
        password_hash = hashlib.sha256(user.password.encode()).hexdigest()
        sql = "INSERT INTO users_auth (username, password_hash) VALUES ({username:String}, {password_hash:String})"
        params = {"username": user.username, "password_hash": password_hash}
        ch.command(sql, parameters=params)
        access_token = create_access_token(data={"sub": user.username})
        logger.info("Người dùng {} đã đăng ký", user.username)
        return {"access_token": access_token, "token_type": "bearer"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi đăng ký: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")


@router.post("/login")
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    ch: ClickHouseClient = Depends(get_ch),
):
    """Đăng nhập và trả về token truy cập."""
    try:
        sql = "SELECT password_hash FROM users_auth WHERE username={username:String}"
        params = {"username": form_data.username}
        result = ch.query(sql, parameters=params)
        rows = result.result_rows
        password_hash = hashlib.sha256(form_data.password.encode()).hexdigest()
        if not rows or rows[0][0] != password_hash:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        access_token = create_access_token(data={"sub": form_data.username})
        logger.info("Người dùng {} đăng nhập", form_data.username)
        return {"access_token": access_token, "token_type": "bearer"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi đăng nhập: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")

