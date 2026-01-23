"""Главный модуль приложения для мониторинга S3 бакета и отправки уведомлений в Kafka"""

import logging
import sys

from src.config import CONFIG
from src.kafka_m import KafkaProducerClient
from src.poller.poller import S3Poller
from src.s3.client import S3Client
from src.utils.logging import setup_logging

logger = logging.getLogger(__name__)


def main() -> None:
    """Главная функция приложения"""
    # Настройка логирования
    setup_logging(level=CONFIG.log_level, kafka_log_level=CONFIG.kafka_log_level)

    logger.info("Запуск S3 Poller")
    logger.info(f"Подключение к S3 бакету: {CONFIG.bucket_name}")
    logger.info(f"Kafka брокеры: {CONFIG.kafka_brokers_list}")
    logger.info(f"Kafka топик: {CONFIG.kafka_topic}")
    logger.info(f"Интервал поллинга: {CONFIG.poll_interval} секунд")
    logger.info(f"Время жизни ссылки на файл: {CONFIG.file_link_expiration} секунд")

    # Инициализация S3 клиента
    s3_client = S3Client(
        key_id=CONFIG.key_id,
        key_secret=CONFIG.key_secret,
        endpoint_url=CONFIG.s3_endpoint,
    )

    # Инициализация Kafka Producer
    kafka_producer = KafkaProducerClient(
        brokers=CONFIG.kafka_brokers_list,
        topic=CONFIG.kafka_topic,
    )

    # Создание и запуск поллера
    poller = S3Poller(
        s3_client=s3_client,
        bucket_name=CONFIG.bucket_name,
        kafka_producer=kafka_producer,
        poll_interval=CONFIG.poll_interval,
        file_link_expiration=CONFIG.file_link_expiration,
    )

    try:
        poller.run()
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Закрываем соединения
        logger.info("Закрытие соединений...")
        kafka_producer.close()
        s3_client.close()
        logger.info("Приложение завершено")


if __name__ == "__main__":
    main()
