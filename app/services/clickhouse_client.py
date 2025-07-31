from clickhouse_connect import get_client
from app.core.config import settings
import time

class ClickHouseClient:
    def __init__(self):
        max_retries = 5
        delay = 1
        for i in range(max_retries):
            try:
                self.client = get_client(
                    host=settings.CLICKHOUSE_HOST,
                    port=settings.CLICKHOUSE_PORT,
                    username=settings.CLICKHOUSE_USER,
                    password=settings.CLICKHOUSE_PASSWORD,
                    database=settings.CLICKHOUSE_DATABASE,
                )
                break
            except Exception as e:
                if i == max_retries - 1:
                    raise
                print(f"Retry {i+1}/{max_retries} to connect to ClickHouse: {e}")
                time.sleep(delay)
                delay = min(delay * 2, 20)

