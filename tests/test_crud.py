import sys
from pathlib import Path

import pytest
from fastapi import HTTPException
from starlette.datastructures import QueryParams


# Đảm bảo thư mục gốc của dự án có trong sys.path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.routers.crud import _schema_dict, query_rows


class FakeClient:
    def get_table_schema(self, table: str):
        raise Exception("Code: 60, DB::Exception: Table default.users doesn't exist")


def test_schema_dict_table_not_found():
    client = FakeClient()
    with pytest.raises(HTTPException) as exc:
        _schema_dict(client, "users")
    assert exc.value.status_code == 404


class FakeQueryClient:
    def __init__(self):
        self.sql = None
        self.parameters = None

    def get_table_schema(self, table: str):
        return [
            ("order_id", "UInt64"),
            ("user_id", "UInt64"),
            ("status", "String"),
        ]

    def query(self, sql: str, parameters=None):
        self.sql = sql
        self.parameters = parameters

        class Result:
            result_rows = [(2, "active")]

        return Result()


class SimpleRequest:
    def __init__(self, qp: QueryParams):
        self.query_params = qp


def test_query_rows_with_aggregate():
    client = FakeQueryClient()
    qp = QueryParams("aggregate=count:order_id&status=active&group_by=status")
    req = SimpleRequest(qp)
    import asyncio

    res = asyncio.get_event_loop().run_until_complete(
        query_rows("fact_orders", req, ch=client)
    )
    assert (
        client.sql
        == "SELECT COUNT(order_id) AS count_order_id, status FROM fact_orders WHERE status={status:String} GROUP BY status"
    )
    assert res == [{"count_order_id": 2, "status": "active"}]
