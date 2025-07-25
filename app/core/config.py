from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    SECRET_KEY: str = "my-secret-key"  # Thay đổi trong production
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    CLICKHOUSE_HOST: str = "clickhouse"
    CLICKHOUSE_PORT: int = 9000  # Thay đổi từ 8123 thành 9000
    CLICKHOUSE_USER: str = "myuser"
    CLICKHOUSE_PASSWORD: str = "my_password"
    CLICKHOUSE_DATABASE: str = "default"

settings = Settings()