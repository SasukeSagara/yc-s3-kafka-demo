import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, TypedDict

from boto3.session import Session as BotoSession
from botocore.client import BaseClient
from botocore.paginate import PageIterator, Paginator
from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.config import CONFIG


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


def get_session(
    key_id: str = CONFIG.key_id,
    key_secret: str = CONFIG.key_secret,
) -> BotoSession:
    """Создает Boto3 сессию с указанными учетными данными"""
    session = BotoSession(
        aws_access_key_id=key_id,
        aws_secret_access_key=key_secret,
    )
    return session


def get_client(session: BotoSession, service_name: str) -> BaseClient:
    """Создает клиент для указанного сервиса AWS/Yandex Cloud"""
    return session.client(
        service_name,
        endpoint_url="https://storage.yandexcloud.net",
    )


def generate_presigned_url(
    client: BaseClient, bucket_name: str, key: str, expiration: int = 3600
) -> str:
    """Генерирует presigned URL для объекта S3"""
    return client.generate_presigned_url(
        "get_object", Params={"Bucket": bucket_name, "Key": key}, ExpiresIn=expiration
    )


def scan_s3_bucket(
    client: BaseClient, bucket_name: str, last_scan_time: datetime
) -> List[Dict[str, Any]]:
    """
    Сканирует S3 бакет и возвращает список файлов, измененных после last_scan_time
    """
    updated_files: List[Dict[str, Any]] = []

    try:
        paginator: Paginator = client.get_paginator("list_objects_v2")
        pages: PageIterator = paginator.paginate(Bucket=bucket_name)

        for page in pages:
            if "Contents" not in page:
                continue

            for obj in page["Contents"]:
                # Проверяем дату последнего изменения
                last_modified = obj["LastModified"]

                # Преобразуем время S3 (обычно UTC) в локальную таймзону машины
                if last_modified.tzinfo is None:
                    # Если timezone не указан, считаем что это UTC
                    last_modified = last_modified.replace(tzinfo=timezone.utc)
                # Преобразуем в локальную таймзону машины
                last_modified_local = last_modified.astimezone()

                # Если файл был изменен после последнего сканирования
                if last_modified_local > last_scan_time:
                    file_info = {
                        "key": obj["Key"],
                        "last_modified": last_modified_local.isoformat(),
                        "size": obj["Size"],
                        "etag": obj["ETag"].strip('"'),
                    }
                    updated_files.append(file_info)

    except Exception as e:
        print(f"Ошибка при сканировании бакета: {e}")

    return updated_files


def send_to_kafka(
    producer: KafkaProducer, topic: str, files: List[Dict[str, Any]]
) -> None:
    """
    Отправляет список файлов в Kafka топик
    """
    if not files:
        print("Нет новых файлов для отправки")
        return

    try:
        # Формируем сообщение со списком файлов
        message: Dict[str, Any] = {
            "timestamp": datetime.now().astimezone().isoformat(),
            "files": files,
            "count": len(files),
        }

        # Сериализуем в JSON
        message_json = json.dumps(message).encode("utf-8")

        # Отправляем в Kafka
        future = producer.send(topic, value=message_json)

        # Ждем подтверждения
        record_metadata = future.get(timeout=10)
        print(
            f"Отправлено {len(files)} файлов в топик {topic}, partition: {record_metadata.partition}, offset: {record_metadata.offset}"
        )

    except KafkaError as e:
        print(f"Ошибка при отправке в Kafka: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")


def poll_s3_bucket(
    client: BaseClient,
    bucket_name: str,
    producer: KafkaProducer,
    topic: str,
    poll_interval: int,
) -> None:
    """
    Основной цикл поллинга S3 бакета
    """
    last_scan_time = datetime.now().astimezone()
    print(f"Поллер запущен. Интервал сканирования: {poll_interval} секунд")
    print(f"Начальное время сканирования: {last_scan_time.isoformat()}")

    try:
        while True:
            print(
                f"\n[{datetime.now().astimezone().isoformat()}] Начало сканирования..."
            )

            # Сканируем бакет
            updated_files = scan_s3_bucket(client, bucket_name, last_scan_time)

            if updated_files:
                print(f"Найдено {len(updated_files)} обновленных файлов:")
                for file_info in updated_files:
                    # Генерируем presigned URL для каждого файла
                    file_info["url"] = generate_presigned_url(
                        client, bucket_name, file_info["key"]
                    )
                    print(
                        f"  - {file_info['key']} (изменен: {file_info['last_modified']})"
                    )

                # Отправляем в Kafka
                send_to_kafka(producer, topic, updated_files)
            else:
                print("Новых файлов не найдено")

            # Обновляем время последнего сканирования
            last_scan_time = datetime.now().astimezone()

            # Ждем перед следующим сканированием
            print(f"Ожидание {poll_interval}с. до следующего сканирования...")
            time.sleep(poll_interval)

    except KeyboardInterrupt:
        print("\nПоллер остановлен пользователем")
    except Exception as e:
        print(f"Критическая ошибка в поллере: {e}")
        raise


def main() -> None:
    # Инициализация S3 клиента
    session = get_session()
    s3_client = get_client(session, "s3")

    # Инициализация Kafka Producer
    kafka_brokers = CONFIG.kafka_brokers.split(",")
    producer = KafkaProducer(
        bootstrap_servers=kafka_brokers,
        value_serializer=lambda v: v,  # Уже сериализовано в JSON
    )

    print(f"Подключение к S3 бакету: {CONFIG.bucket_name}")
    print(f"Kafka брокеры: {CONFIG.kafka_brokers}")
    print(f"Kafka топик: {CONFIG.kafka_topic}")

    # Запускаем поллер
    poll_s3_bucket(
        client=s3_client,
        bucket_name=CONFIG.bucket_name,
        producer=producer,
        topic=CONFIG.kafka_topic,
        poll_interval=CONFIG.poll_interval,
    )

    # Закрываем соединения
    producer.close()


if __name__ == "__main__":
    main()
