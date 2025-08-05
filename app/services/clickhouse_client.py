from clickhouse_connect import get_client
from app.core.config import settings
import backoff
import logging

logger = logging.getLogger(__name__)


class ClickHouseClient:
    def __init__(self):
        self.client = self._connect()

    @backoff.on_exception(backoff.expo, Exception, max_time=20, jitter=None)
    def _connect(self):
        logger.info("Connecting to ClickHouse")
        return get_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            username=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            database=settings.CLICKHOUSE_DATABASE,
        )

    def command(self, sql: str):
        return self.client.command(sql)

    def query(self, sql: str):
        return self.client.query(sql)

    def init_db(self):
        self.command(
            """
            CREATE TABLE IF NOT EXISTS dim_users (
                id UInt64,
                name String,
                email String
            ) ENGINE = MergeTree()
            ORDER BY id
            """
        )
        self.command(
            """
            CREATE TABLE IF NOT EXISTS dim_products (
                id UInt64,
                name String
            ) ENGINE = MergeTree()
            ORDER BY id
            """
        )
        self.command(
            """
            CREATE TABLE IF NOT EXISTS fact_orders (
                order_id UInt64,
                user_id UInt64,
                product_id UInt64,
                quantity UInt32,
                total Float64,
                order_date DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            ORDER BY order_id
            """
        )

