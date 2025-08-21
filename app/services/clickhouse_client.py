"""Client tiện ích để kết nối và thao tác với ClickHouse."""

from clickhouse_connect import get_client
from app.core.config import settings
import backoff
from loguru import logger


class ClickHouseClient:
    """Bao bọc client ClickHouse và cung cấp các phương thức tiện ích."""

    def __init__(self):
        """Khởi tạo kết nối tới ClickHouse."""
        self.client = self._connect()

    @backoff.on_exception(backoff.expo, Exception, max_time=20, jitter=None)
    def _connect(self):
        """Tạo kết nối tới máy chủ ClickHouse."""
        logger.info("Kết nối tới ClickHouse")
        return get_client(
            host=settings.CLICKHOUSE_HOST,
            port=settings.CLICKHOUSE_PORT,
            username=settings.CLICKHOUSE_USER,
            password=settings.CLICKHOUSE_PASSWORD,
            database=settings.CLICKHOUSE_DATABASE,
        )

    def command(self, sql: str, parameters: dict | None = None):
        """Thực thi câu lệnh không phải ``SELECT``.

        Tham số có thể truyền vào để thay thế động trong truy vấn, giúp API
        hoạt động với các câu lệnh tùy ý mà không cần định dạng chuỗi.
        """
        try:
            return self.client.command(sql, parameters=parameters or {})
        except Exception as exc:
            logger.exception("Lỗi khi thực thi command: {}", exc)
            raise

    def query(self, sql: str, parameters: dict | None = None):
        """Thực thi câu lệnh ``SELECT`` và trả về kết quả thô từ ClickHouse."""
        try:
            return self.client.query(sql, parameters=parameters or {})
        except Exception as exc:
            logger.exception("Lỗi khi thực thi query: {}", exc)
            raise

    def init_db(self):
        """Khởi tạo các bảng cần thiết nếu chưa tồn tại."""
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

