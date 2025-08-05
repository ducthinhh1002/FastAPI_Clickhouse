from fastapi import APIRouter, Depends, HTTPException, Request
from app.models.warehouse import User
from app.services.clickhouse_client import ClickHouseClient


def get_ch(request: Request) -> ClickHouseClient:
    return request.app.state.clickhouse


router = APIRouter(prefix="/users", tags=["users"])


@router.post("/")
async def create_user(user: User, ch: ClickHouseClient = Depends(get_ch)):
    # Check for existing ID to avoid duplicates
    check_sql = "SELECT count() FROM dim_users WHERE id={id:UInt64}"
    exists = ch.client.query(check_sql, parameters={"id": user.id}).result_rows[0][0]
    if exists:
        raise HTTPException(status_code=400, detail="User with this id already exists")

    sql = "INSERT INTO dim_users (id, name, email) VALUES ({id:UInt64}, {name:String}, {email:String})"
    params = {"id": user.id, "name": user.name, "email": user.email}
    ch.client.command(sql, parameters=params)
    return {"status": "ok"}


@router.get("/{user_id}", response_model=User)
async def read_user(user_id: int, ch: ClickHouseClient = Depends(get_ch)):
    sql = "SELECT id, name, email FROM dim_users WHERE id = {user_id:UInt64}"
    params = {"user_id": user_id}
    result = ch.client.query(sql, parameters=params)
    rows = result.result_rows
    if not rows:
        raise HTTPException(status_code=404, detail="User not found")
    r = rows[0]
    return User(id=r[0], name=r[1], email=r[2])


@router.put("/{user_id}")
async def update_user(user_id: int, user: User, ch: ClickHouseClient = Depends(get_ch)):
    sql = "ALTER TABLE dim_users UPDATE name={name:String}, email={email:String} WHERE id={user_id:UInt64}"
    params = {"name": user.name, "email": user.email, "user_id": user_id}
    ch.client.command(sql, parameters=params)
    return {"status": "ok"}


@router.delete("/{user_id}")
async def delete_user(user_id: int, ch: ClickHouseClient = Depends(get_ch)):
    sql = "ALTER TABLE dim_users DELETE WHERE id={user_id:UInt64}"
    params = {"user_id": user_id}
    ch.client.command(sql, parameters=params)
    return {"status": "ok"}
