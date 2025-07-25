from clickhouse_driver import Client
from app.core.config import settings
import time

class ClickHouseClient:
    def __init__(self):
        max_retries = 20  
        for i in range(max_retries):
            try:
                self.client = Client(
                    host=settings.CLICKHOUSE_HOST,
                    port=settings.CLICKHOUSE_PORT,
                    user=settings.CLICKHOUSE_USER,
                    password=settings.CLICKHOUSE_PASSWORD,
                    database=settings.CLICKHOUSE_DATABASE
                )
                self._init_table()
                break
            except Exception as e:
                if i == max_retries - 1:
                    raise
                time.sleep(20)  # Tăng lên 20 giây chờ giữa các lần thử
                print(f"Retry {i+1}/{max_retries} to connect to ClickHouse: {str(e)}")

    def _init_table(self):
        self.client.execute("""
            CREATE TABLE IF NOT EXISTS articles (
                id UInt32,
                title String,
                content String,
                created_at DateTime
            ) ENGINE = MergeTree()
            ORDER BY created_at
        """)

    def insert_article(self, title: str, content: str):
        return self.client.execute(
            "INSERT INTO articles (id, title, content, created_at) VALUES",
            [(self._get_next_id(), title, content, 'now()')]
        )

    def get_articles(self, skip: int = 0, limit: int = 10):
        return self.client.execute(
            "SELECT id, title, content, created_at FROM articles LIMIT %s OFFSET %s",
            (limit, skip)
        )

    def get_article(self, article_id: int):
        result = self.client.execute(
            "SELECT id, title, content, created_at FROM articles WHERE id = %s",
            (article_id,)
        )
        return result[0] if result else None

    def update_article(self, article_id: int, title: str, content: str):
        return self.client.execute(
            "ALTER TABLE articles UPDATE title = %s, content = %s WHERE id = %s",
            (title, content, article_id)
        )

    def delete_article(self, article_id: int):
        return self.client.execute(
            "ALTER TABLE articles DELETE WHERE id = %s",
            (article_id,)
        )

    def _get_next_id(self):
        result = self.client.execute("SELECT max(id) FROM articles")
        return (result[0][0] + 1) if result[0][0] else 1

clickhouse_client = ClickHouseClient()