"""Поллер для периодического сканирования S3 бакета"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List

from src.kafka_m import KafkaProducerClient
from src.s3.client import S3Client
from src.s3.scanner import S3Scanner

logger = logging.getLogger(__name__)


class S3Poller:
    """Поллер для периодического сканирования S3 бакета и отправки обновлений в Kafka"""

    def __init__(
        self,
        s3_client: S3Client,
        bucket_name: str,
        kafka_producer: KafkaProducerClient,
        poll_interval: int = 60,
        file_link_expiration: int = 3600,
    ):
        """
        Инициализация поллера

        Args:
            s3_client: Клиент для работы с S3
            bucket_name: Имя бакета для сканирования
            kafka_producer: Клиент для отправки сообщений в Kafka
            poll_interval: Интервал сканирования в секундах
            file_link_expiration: Время жизни presigned URL в секундах
        """
        self._s3_client = s3_client
        self._bucket_name = bucket_name
        self._kafka_producer = kafka_producer
        self._poll_interval = poll_interval
        self._file_link_expiration = file_link_expiration
        self._scanner: S3Scanner | None = None

    def _get_scanner(self) -> S3Scanner:
        """Создает или возвращает существующий сканер"""
        if self._scanner is None:
            client = self._s3_client.get_client()
            self._scanner = S3Scanner(client, self._bucket_name)
        return self._scanner

    def run(self) -> None:
        """Запускает основной цикл поллинга"""
        last_scan_time = datetime.now().astimezone()
        logger.info(
            f"Поллер запущен. Интервал сканирования: {self._poll_interval} секунд"
        )
        logger.info(f"Начальное время сканирования: {last_scan_time.isoformat()}")

        try:
            while True:
                logger.debug(
                    f"[{datetime.now().astimezone().isoformat()}] Начало сканирования..."
                )

                # Сканируем бакет
                scanner = self._get_scanner()
                updated_files = scanner.scan_updated_files(last_scan_time)

                if updated_files:
                    logger.info(f"Найдено {len(updated_files)} обновленных файлов:")
                    self._process_files(updated_files)

                    # Отправляем в Kafka
                    self._kafka_producer.send_files(updated_files)
                else:
                    logger.debug("Новых файлов не найдено")

                # Обновляем время последнего сканирования
                last_scan_time = datetime.now().astimezone()

                # Ждем перед следующим сканированием
                logger.debug(
                    f"Ожидание {self._poll_interval}с. до следующего сканирования..."
                )
                time.sleep(self._poll_interval)

        except KeyboardInterrupt:
            logger.info("Поллер остановлен пользователем")
        except Exception as e:
            logger.error(f"Критическая ошибка в поллере: {e}", exc_info=True)
            raise

    def _process_files(self, files: List[Dict[str, Any]]) -> None:
        """
        Обрабатывает файлы: генерирует presigned URL и логирует информацию

        Args:
            files: Список файлов для обработки
        """
        for file_info in files:
            # Генерируем presigned URL для каждого файла
            file_info["url"] = self._s3_client.generate_presigned_url(
                self._bucket_name, file_info["key"], expiration=self._file_link_expiration
            )
            logger.info(f"\t{file_info['key']} (изменен: {file_info['last_modified']})")
