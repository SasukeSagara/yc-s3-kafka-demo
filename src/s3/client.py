"""Клиент для работы с S3"""

import logging
import ssl
import urllib.request
from pathlib import Path
from typing import Optional

from boto3.session import Session as BotoSession
from botocore.client import BaseClient
from botocore.config import Config

from src.s3._sigv4_unsigned import build_put_url_and_path, sign_s3_put_unsigned

logger = logging.getLogger(__name__)

# Конфиг S3: отключаем подпись payload для операций через boto3 (presigned URL и т.д.)
_S3_CONFIG = Config(s3={"payload_signing_enabled": False})


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

        Используется HTTP PUT с подписью SigV4 и x-amz-content-sha256: UNSIGNED-PAYLOAD
        для совместимости с Yandex Object Storage и другими S3-совместимыми бэкендами.

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

        full_url, host, canonical_uri = build_put_url_and_path(
            self._endpoint_url, bucket_name, key
        )
        _, _, headers = sign_s3_put_unsigned(
            key_id=self._key_id,
            key_secret=self._key_secret,
            method="PUT",
            url_path=canonical_uri,
            host=host,
        )
        headers["Content-Length"] = str(path.stat().st_size)

        body = path.read_bytes()
        req = urllib.request.Request(
            full_url,
            data=body,
            method="PUT",
            headers=headers,
        )
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(req, context=ssl_ctx) as resp:
            if resp.status not in (200, 204):
                raise OSError(f"PUT {full_url}: HTTP {resp.status}")

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
