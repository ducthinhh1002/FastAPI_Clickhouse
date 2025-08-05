from fastapi import APIRouter, Depends, HTTPException, Request
from app.models.warehouse import User
from app.services.clickhouse_client import ClickHouseClient


def get_ch(request: Request) -> ClickHouseClient:
    return request.app.state.clickhouse


router = APIRouter(prefix="/users", tags=["users"])


@router.post("/")
async def create_user(user: User, ch: ClickHouseClient = Depends(get_ch)):
    ch.command(f"INSERT INTO dim_users (id, name, email) VALUES ({user.id}, '{user.name}', '{user.email}')")
    return {"status": "ok"}


@router.get("/{user_id}", response_model=User)
async def read_user(user_id: int, ch: ClickHouseClient = Depends(get_ch)):
    result = ch.query(f"SELECT id, name, email FROM dim_users WHERE id = {user_id}")
    rows = result.result_rows
    if not rows:
        raise HTTPException(status_code=404, detail="User not found")
    r = rows[0]
    return User(id=r[0], name=r[1], email=r[2])


@router.put("/{user_id}")
async def update_user(user_id: int, user: User, ch: ClickHouseClient = Depends(get_ch)):
    ch.command(f"ALTER TABLE dim_users UPDATE name='{user.name}', email='{user.email}' WHERE id={user_id}")
    return {"status": "ok"}


@router.delete("/{user_id}")
async def delete_user(user_id: int, ch: ClickHouseClient = Depends(get_ch)):
    ch.command(f"ALTER TABLE dim_users DELETE WHERE id={user_id}")
    return {"status": "ok"}
