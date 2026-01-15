from pydantic_settings import BaseSettings, SettingsConfigDict


class Configuration(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    key_id: str
    key_secret: str
    bucket_name: str
    account_id: str
    # Интервал поллинга в секундах
    poll_interval: int = 60
    # Настройки Kafka
    kafka_brokers: str
    kafka_topic: str


CONFIG = Configuration()
