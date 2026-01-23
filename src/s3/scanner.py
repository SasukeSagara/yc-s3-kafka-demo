"""Сканер для S3 бакета"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from botocore.client import BaseClient  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]
from botocore.paginate import PageIterator, Paginator  # type: ignore[import-untyped]

from src.utils.retry import retry

logger = logging.getLogger(__name__)


class S3Scanner:
    """Сканер для поиска измененных файлов в S3 бакете"""

    def __init__(self, client: BaseClient, bucket_name: str):
        """
        Инициализация сканера

        Args:
            client: S3 клиент
            bucket_name: Имя бакета для сканирования
        """
        self._client = client
        self._bucket_name = bucket_name

    @retry(max_attempts=3, delay=1.0, backoff=2.0, exceptions=(ClientError, Exception))
    def scan_updated_files(self, last_scan_time: datetime) -> List[Dict[str, Any]]:
        """
        Сканирует S3 бакет и возвращает список файлов, измененных после last_scan_time

        Args:
            last_scan_time: Время последнего сканирования

        Returns:
            Список словарей с информацией о файлах (без URL)
        """
        updated_files: List[Dict[str, Any]] = []

        try:
            paginator: Paginator = self._client.get_paginator("list_objects_v2")
            pages: PageIterator = paginator.paginate(Bucket=self._bucket_name)

            for page in pages:
                if "Contents" not in page:
                    continue

                for obj in page["Contents"]:
                    file_info = self._process_object(obj, last_scan_time)
                    if file_info:
                        updated_files.append(file_info)

            logger.debug(f"Найдено {len(updated_files)} обновленных файлов")

        except Exception as e:
            logger.error(f"Ошибка при сканировании бакета: {e}", exc_info=True)
            raise

        return updated_files

    def _process_object(
        self, obj: Dict[str, Any], last_scan_time: datetime
    ) -> Dict[str, Any] | None:
        """
        Обрабатывает объект S3 и возвращает информацию о файле, если он был изменен

        Args:
            obj: Объект из ответа S3
            last_scan_time: Время последнего сканирования

        Returns:
            Словарь с информацией о файле или None, если файл не был изменен
        """
        last_modified = obj["LastModified"]

        # Преобразуем время S3 (обычно UTC) в локальную таймзону машины
        if last_modified.tzinfo is None:
            # Если timezone не указан, считаем что это UTC
            last_modified = last_modified.replace(tzinfo=timezone.utc)
        # Преобразуем в локальную таймзону машины
        last_modified_local = last_modified.astimezone()

        # Если файл был изменен после последнего сканирования
        if last_modified_local > last_scan_time:
            file_info: Dict[str, Any] = {
                "key": obj["Key"],
                "last_modified": last_modified_local.isoformat(),
                "size": obj["Size"],
                "etag": obj["ETag"].strip('"'),
            }
            return file_info

        return None
