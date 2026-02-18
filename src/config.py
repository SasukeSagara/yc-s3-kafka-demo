"""Конфигурация приложения"""

from pydantic import Field, field_validator  # type: ignore[import-untyped]
from pydantic_settings import (  # type: ignore[import-untyped]
    BaseSettings,
    SettingsConfigDict,
)


class Configuration(BaseSettings):
    """Конфигурация приложения"""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    # Yandex Cloud S3 настройки
    key_id: str = Field(..., description="Access Key ID для Yandex Cloud")
    key_secret: str = Field(..., description="Secret Access Key для Yandex Cloud")
    bucket_name: str = Field(..., description="Имя S3 бакета для мониторинга")
    account_id: str = Field(..., description="ID аккаунта Yandex Cloud")
    s3_endpoint: str = Field(
        default="https://storage.yandexcloud.net",
        description="URL endpoint для S3",
    )

    # Настройки поллинга
    poll_interval: int = Field(
        default=60,
        ge=1,
        description="Интервал поллинга в секундах",
    )

    # Время жизни сгенерированной ссылки на файл (presigned URL)
    file_link_expiration: int = Field(
        default=3600,
        ge=1,
        description="Время жизни presigned URL в секундах (по умолчанию 1 час)",
    )

    # Настройки Kafka (опционально: если не указаны — уведомления в Kafka не отправляются)
    kafka_brokers: str | None = Field(
        default=None,
        description="Список Kafka брокеров через запятую (пусто — Kafka не используется)",
    )
    kafka_topic: str | None = Field(
        default=None,
        description="Имя Kafka топика для отправки уведомлений",
    )

    # Настройки логирования
    log_level: str = Field(
        default="INFO",
        description="Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    kafka_log_level: str | None = Field(
        default=None,
        description="Уровень для логгеров kafka.* (kafka.conn и др.). Пусто — как log_level.",
    )

    @field_validator("log_level", "kafka_log_level")
    @classmethod
    def validate_log_level(cls, v: str | None) -> str | None:
        """Валидирует уровень логирования"""
        if v is None:
            return None
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"log_level должен быть одним из: {valid_levels}")
        return v_upper

    @property
    def kafka_brokers_list(self) -> list[str]:
        """Возвращает список Kafka брокеров (пустой список, если Kafka не настроен)"""
        if not self.kafka_brokers:
            return []
        return [broker.strip() for broker in self.kafka_brokers.split(",")]

    @property
    def kafka_enabled(self) -> bool:
        """True, если заданы и брокеры, и топик — тогда Kafka используется."""
        return bool(self.kafka_brokers and self.kafka_topic)


CONFIG = Configuration()
