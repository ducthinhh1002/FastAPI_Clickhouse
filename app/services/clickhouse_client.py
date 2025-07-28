from clickhouse_driver import Client
from app.core.config import settings
from datetime import datetime
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
        self.client.execute(
            """
            CREATE TABLE IF NOT EXISTS dim_customer (
                id UInt32,
                name String
            ) ENGINE = MergeTree()
            ORDER BY id
            """
        )
        self.client.execute(
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
        result = self.client.execute(f"SELECT max(id) FROM {table}")
        return (result[0][0] + 1) if result[0][0] else 1

    # Customer operations
    def insert_customer(self, name: str):
        return self.client.execute(
            "INSERT INTO dim_customer (id, name) VALUES",
            [(self._get_next_id('dim_customer'), name)],
        )

    def get_customers(self, skip: int = 0, limit: int = 10):
        return self.client.execute(
            "SELECT id, name FROM dim_customer LIMIT %s OFFSET %s",
            (limit, skip),
        )

    def get_customer(self, customer_id: int):
        result = self.client.execute(
            "SELECT id, name FROM dim_customer WHERE id = %s",
            (customer_id,),
        )
        return result[0] if result else None

    def update_customer(self, customer_id: int, name: str):
        return self.client.execute(
            "ALTER TABLE dim_customer UPDATE name = %s WHERE id = %s",
            (name, customer_id),
        )

    def delete_customer(self, customer_id: int):
        return self.client.execute(
            "ALTER TABLE dim_customer DELETE WHERE id = %s",
            (customer_id,),
        )

    # Order operations
    def insert_order(self, customer_id: int, amount: float):
        return self.client.execute(
            "INSERT INTO fact_order (id, customer_id, amount, created_at) VALUES",
            [(
                self._get_next_id('fact_order'),
                customer_id,
                amount,
                datetime.utcnow(),
            )],
        )

    def get_orders(self, skip: int = 0, limit: int = 10):
        return self.client.execute(
            "SELECT id, customer_id, amount, created_at FROM fact_order LIMIT %s OFFSET %s",
            (limit, skip),
        )

    def get_order(self, order_id: int):
        result = self.client.execute(
            "SELECT id, customer_id, amount, created_at FROM fact_order WHERE id = %s",
            (order_id,),
        )
        return result[0] if result else None

    def update_order(self, order_id: int, customer_id: int, amount: float):
        return self.client.execute(
            "ALTER TABLE fact_order UPDATE customer_id = %s, amount = %s WHERE id = %s",
            (customer_id, amount, order_id),
        )

    def delete_order(self, order_id: int):
        return self.client.execute(
            "ALTER TABLE fact_order DELETE WHERE id = %s",
            (order_id,),
        )
