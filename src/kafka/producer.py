"""Kafka Producer клиент"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List

from kafka import KafkaProducer  # type: ignore[import-untyped]
from kafka.errors import KafkaError  # type: ignore[import-untyped]
from src.models.types import KafkaMessage
from src.utils.retry import retry

logger = logging.getLogger(__name__)


class KafkaProducerClient:
    """Клиент для отправки сообщений в Kafka"""

    def __init__(self, brokers: List[str], topic: str):
        """
        Инициализация Kafka Producer

        Args:
            brokers: Список адресов Kafka брокеров
            topic: Имя топика для отправки сообщений
        """
        self._brokers = brokers
        self._topic = topic
        self._producer: KafkaProducer | None = None

    def _get_producer(self) -> KafkaProducer:
        """Создает или возвращает существующий Kafka Producer"""
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self._brokers,
                value_serializer=lambda v: v,  # Уже сериализовано в JSON
            )
            logger.debug(f"Kafka Producer создан для брокеров: {self._brokers}")
        return self._producer

    @retry(max_attempts=3, delay=1.0, backoff=2.0, exceptions=(KafkaError, Exception))
    def send_files(self, files: List[Dict[str, Any]]) -> None:
        """
        Отправляет список файлов в Kafka топик

        Args:
            files: Список словарей с информацией о файлах
        """
        if not files:
            logger.debug("Нет новых файлов для отправки")
            return

        try:
            message: KafkaMessage = {
                "timestamp": datetime.now().astimezone().isoformat(),
                "files": files,  # type: ignore
                "count": len(files),
            }

            # Сериализуем в JSON
            message_json = json.dumps(message).encode("utf-8")

            # Отправляем в Kafka
            producer = self._get_producer()
            future = producer.send(self._topic, value=message_json)

            # Ждем подтверждения
            record_metadata = future.get(timeout=10)
            logger.info(
                f"Отправлено {len(files)} файлов в топик {self._topic}, "
                f"partition: {record_metadata.partition}, "
                f"offset: {record_metadata.offset}"
            )

        except KafkaError as e:
            logger.error(f"Ошибка при отправке в Kafka: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отправке в Kafka: {e}", exc_info=True)
            raise

    def close(self) -> None:
        """Закрывает соединение с Kafka"""
        if self._producer:
            self._producer.close()
            self._producer = None
            logger.debug("Kafka Producer закрыт")
