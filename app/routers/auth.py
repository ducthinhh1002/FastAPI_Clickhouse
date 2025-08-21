from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel
from loguru import logger

from app.core.security import create_access_token

router = APIRouter(prefix="/auth", tags=["auth"])

fake_users_db = {
    "admin": {
        "username": "admin",
        "password": "password",
    }
}


class RegisterForm(BaseModel):
    """Model dữ liệu cho việc đăng ký người dùng."""

    username: str
    password: str


@router.post("/register")
async def register(user: RegisterForm):
    """Đăng ký tài khoản mới và trả về token truy cập."""
    try:
        if user.username in fake_users_db:
            raise HTTPException(status_code=400, detail="Username already registered")
        fake_users_db[user.username] = {
            "username": user.username,
            "password": user.password,
        }
        access_token = create_access_token(data={"sub": user.username})
        logger.info("Người dùng {} đã đăng ký", user.username)
        return {"access_token": access_token, "token_type": "bearer"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi đăng ký: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")


@router.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    """Đăng nhập và trả về token truy cập."""
    try:
        user = fake_users_db.get(form_data.username)
        if not user or user["password"] != form_data.password:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )
        access_token = create_access_token(data={"sub": user["username"]})
        logger.info("Người dùng {} đăng nhập", user["username"])
        return {"access_token": access_token, "token_type": "bearer"}
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Lỗi đăng nhập: {}", exc)
        raise HTTPException(status_code=500, detail="Lỗi máy chủ")
