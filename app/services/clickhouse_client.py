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

