from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    SECRET_KEY: str = "my-secret-key"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    CLICKHOUSE_HOST: str = "clickhouse"
    CLICKHOUSE_PORT: int = 9000
    CLICKHOUSE_USER: str = "admin"
    CLICKHOUSE_PASSWORD: str = "password"
    CLICKHOUSE_DATABASE: str = "default"

settings = Settings()
