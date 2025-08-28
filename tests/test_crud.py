import sys
from pathlib import Path

import pytest
from fastapi import HTTPException


# Đảm bảo thư mục gốc của dự án có trong sys.path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from app.routers.crud import _schema_dict


class FakeClient:
    def get_table_schema(self, table: str):
        raise Exception("Code: 60, DB::Exception: Table default.users doesn't exist")


def test_schema_dict_table_not_found():
    client = FakeClient()
    with pytest.raises(HTTPException) as exc:
        _schema_dict(client, "users")
    assert exc.value.status_code == 404
