from clickhouse_connect import get_client
from app.core.config import settings
from datetime import datetime
import time

class ClickHouseClient:
    def __init__(self):
        max_retries = 20
        for i in range(max_retries):
            try:
                self.client = get_client(
                    host=settings.CLICKHOUSE_HOST,
                    port=settings.CLICKHOUSE_PORT,
                    username=settings.CLICKHOUSE_USER,
                    password=settings.CLICKHOUSE_PASSWORD,
                    database=settings.CLICKHOUSE_DATABASE,
                )
                self._init_tables()
                break
            except Exception as e:
                if i == max_retries - 1:
                    raise
                time.sleep(20)
                print(f"Retry {i+1}/{max_retries} to connect to ClickHouse: {str(e)}")

    def _init_tables(self):
        self.client.command(
            """
            CREATE TABLE IF NOT EXISTS dim_customer (
                id UInt32,
                name String
            ) ENGINE = MergeTree()
            ORDER BY id
            """
        )
        self.client.command(
            """
            CREATE TABLE IF NOT EXISTS fact_order (
                id UInt32,
                customer_id UInt32,
                amount Float32,
                created_at DateTime
            ) ENGINE = MergeTree()
            ORDER BY created_at
            """
        )

    def _get_next_id(self, table: str):
        result = self.client.query(f"SELECT max(id) FROM {table}")
        max_id = result.result_rows[0][0] if result.result_rows[0][0] is not None else 0
        return max_id + 1

    # Customer operations
    def insert_customer(self, name: str):
        self.client.insert(
            'dim_customer',
            [(self._get_next_id('dim_customer'), name)],
            column_names=['id', 'name'],
        )

    def get_customers(self, skip: int = 0, limit: int = 10):
        result = self.client.query(
            "SELECT id, name FROM dim_customer LIMIT %(limit)s OFFSET %(skip)s",
            parameters={'limit': limit, 'skip': skip},
        )
        return result.result_rows

    def get_customer(self, customer_id: int):
        result = self.client.query(
            "SELECT id, name FROM dim_customer WHERE id = %(id)s",
            parameters={'id': customer_id},
        )
        return result.result_rows[0] if result.result_rows else None

    def update_customer(self, customer_id: int, name: str):
        self.client.command(
            "ALTER TABLE dim_customer UPDATE name = %(name)s WHERE id = %(id)s",
            parameters={'name': name, 'id': customer_id},
        )

    def delete_customer(self, customer_id: int):
        self.client.command(
            "ALTER TABLE dim_customer DELETE WHERE id = %(id)s",
            parameters={'id': customer_id},
        )

    # Order operations
    def insert_order(self, customer_id: int, amount: float):
        self.client.insert(
            'fact_order',
            [(
                self._get_next_id('fact_order'),
                customer_id,
                amount,
                datetime.utcnow(),
            )],
            column_names=['id', 'customer_id', 'amount', 'created_at'],
        )

    def get_orders(self, skip: int = 0, limit: int = 10):
        result = self.client.query(
            "SELECT id, customer_id, amount, created_at FROM fact_order LIMIT %(limit)s OFFSET %(skip)s",
            parameters={'limit': limit, 'skip': skip},
        )
        return result.result_rows

    def get_order(self, order_id: int):
        result = self.client.query(
            "SELECT id, customer_id, amount, created_at FROM fact_order WHERE id = %(id)s",
            parameters={'id': order_id},
        )
        return result.result_rows[0] if result.result_rows else None

    def update_order(self, order_id: int, customer_id: int, amount: float):
        self.client.command(
            "ALTER TABLE fact_order UPDATE customer_id = %(cid)s, amount = %(amt)s WHERE id = %(id)s",
            parameters={'cid': customer_id, 'amt': amount, 'id': order_id},
        )

    def delete_order(self, order_id: int):
        self.client.command(
            "ALTER TABLE fact_order DELETE WHERE id = %(id)s",
            parameters={'id': order_id},
        )
