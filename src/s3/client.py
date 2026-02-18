"""Клиент для работы с S3"""

import logging
from pathlib import Path
from typing import Optional

from boto3.session import Session as BotoSession
from botocore.client import BaseClient
from botocore.config import Config

logger = logging.getLogger(__name__)

# Конфиг S3: отключаем подпись payload, чтобы отправлять x-amz-content-sha256: UNSIGNED-PAYLOAD
# (требуется для части S3-совместимых бэкендов: Yandex Object Storage, MinIO за прокси и др.)
_S3_CONFIG = Config(s3={"payload_signing_enabled": False})


class _UnseekableReader:
    """Обёртка над файлом без seek(), чтобы boto3 отправлял x-amz-content-sha256: UNSIGNED-PAYLOAD (совместимость с Yandex Object Storage и др.)."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._size = path.stat().st_size
        self._file = open(path, "rb")

    def read(self, size: int = -1) -> bytes:
        return self._file.read(size)

    def __len__(self) -> int:
        return self._size

    def __enter__(self) -> "_UnseekableReader":
        return self

    def __exit__(self, *args: object) -> None:
        self._file.close()

    def __iter__(self) -> "_UnseekableReader":
        return self

    def __next__(self) -> bytes:
        chunk = self._file.read(8192)
        if not chunk:
            raise StopIteration
        return chunk


class S3Client:
    """Клиент для работы с S3 бакетом Yandex Cloud"""

    def __init__(
        self,
        key_id: str,
        key_secret: str,
        endpoint_url: str = "https://storage.yandexcloud.net",
    ):
        """
        Инициализация S3 клиента

        Args:
            key_id: Access Key ID для Yandex Cloud
            key_secret: Secret Access Key для Yandex Cloud
            endpoint_url: URL endpoint для S3 (по умолчанию Yandex Cloud)
        """
        self._key_id = key_id
        self._key_secret = key_secret
        self._endpoint_url = endpoint_url
        self._session: Optional[BotoSession] = None
        self._client: Optional[BaseClient] = None

    def _get_session(self) -> BotoSession:
        """Создает или возвращает существующую Boto3 сессию"""
        if self._session is None:
            self._session = BotoSession(
                aws_access_key_id=self._key_id,
                aws_secret_access_key=self._key_secret,
            )
        return self._session

    def get_client(self) -> BaseClient:
        """Создает или возвращает существующий S3 клиент"""
        if self._client is None:
            session = self._get_session()
            self._client = session.client(
                "s3",
                endpoint_url=self._endpoint_url,
                verify=False,
                config=_S3_CONFIG,
            )
            logger.debug("S3 клиент создан")
        return self._client

    def upload_file(
        self, bucket_name: str, local_path: str, object_key: str | None = None
    ) -> str:
        """
        Загружает файл в S3 бакет.

        Используется put_object с потоком без seek(), чтобы отправлять
        x-amz-content-sha256: UNSIGNED-PAYLOAD для совместимости с Yandex Object Storage
        и другими S3-совместимыми бэкендами.

        Args:
            bucket_name: Имя бакета
            local_path: Путь к локальному файлу
            object_key: Ключ объекта в S3 (если не указан — используется имя файла)

        Returns:
            Ключ загруженного объекта в S3
        """
        path = Path(local_path)
        if not path.is_file():
            raise FileNotFoundError(f"Файл не найден: {local_path}")
        key = object_key if object_key is not None else path.name
        client = self.get_client()
        size = path.stat().st_size
        with _UnseekableReader(path) as body:
            client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=body,
                ContentLength=size,
            )
        logger.info(f"Файл загружен: {local_path} -> s3://{bucket_name}/{key}")
        return key

    def generate_presigned_url(
        self, bucket_name: str, key: str, expiration: int = 3600
    ) -> str:
        """
        Генерирует presigned URL для объекта S3

        Args:
            bucket_name: Имя бакета
            key: Ключ объекта
            expiration: Время жизни URL в секундах (по умолчанию 1 час)

        Returns:
            Presigned URL для доступа к объекту
        """
        client = self.get_client()
        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": key},
            ExpiresIn=expiration,
        )
        logger.debug(f"Сгенерирован presigned URL для {key}")
        return url

    def close(self) -> None:
        """Закрывает соединения"""
        if self._client:
            self._client = None
        if self._session:
            self._session = None
        logger.debug("S3 клиент закрыт")
