from pydantic_settings import BaseSettings
from pydantic_settings import SettingsConfigDict
from pydantic import BaseModel


class KafkaConfig(BaseModel):
    url: str = ""
    topic_prefix: str = ""


class Settings(BaseSettings):
    CLICKHOUSE_HOST: str = "localhost"
    CLICKHOUSE_HTTP_PORT: str = "8124"
    CLICKHOUSE_USERNAME: str = "default"
    CLICKHOUSE_PASSWORD: str = ""
    CLICKHOUSE_DATABASE: str = "default"

    MT_SERVER: str = "37.27.126.212:443"
    MT_GROUPS: str = "demo\\duc_dev\\account_metrics"
    MT_LOGIN: int = "1009"
    MT_PASSWORD: str = "Tung@0012"
    
    SERVER_NAME: str = "demo"


    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        str_strip_whitespace=True,
        validate_assignment=True,
    )

    INTERVAL: float = 0.5

    NATS_CONNECTION: str = "nats://localhost:4222"
    TOPIC_PREFIX: str = "test."

    KAFKA_BOOTSTRAP_SERVERS_URL: str = "localhost:9092"
    KAFKA_PRODUCER_SSL_CA: str | None = None
    KAFKA_PRODUCER_SSL_CERTIFICATE: str | None = None
    KAFKA_PRODUCER_SSL_KEY: str | None = None
    KAFKA_TOPIC_PREFIX: str = "test."
    KAFKA_CLIENT_ID: str = "test"


settings = Settings()

type_map = {
    "int": "Int64",
    "float": "Float64",
    "str": "String",
    "date": "Date32",
    "datetime": "DateTime64",
}

MIN_TIME = 31536000
