"""Ứng dụng cấu hình và thiết lập môi trường."""

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Cấu hình toàn cục cho ứng dụng."""

    SECRET_KEY: str = "my-secret-key"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    # Mặc định trỏ tới máy cục bộ để tránh lỗi không resolve được DNS
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_PORT: int = 8123
    CLICKHOUSE_USER: str = "admin"
    CLICKHOUSE_PASSWORD: str = "password"
    CLICKHOUSE_DATABASE: str = "default"


settings = Settings()
