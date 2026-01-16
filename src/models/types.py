"""Типы данных для проекта"""

from typing import List, TypedDict


class FileInfo(TypedDict):
    """Информация о файле из S3"""

    key: str
    last_modified: str
    size: int
    etag: str
    url: str


class KafkaMessage(TypedDict):
    """Структура сообщения для Kafka"""

    timestamp: str
    files: List[FileInfo]
    count: int
