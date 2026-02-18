"""Скрипт для загрузки файла в S3 бакет Yandex Cloud"""

import argparse
import logging
import sys

from src.config import CONFIG
from src.s3.client import S3Client
from src.utils.logging import setup_logging

logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Загрузить файл в S3 бакет Yandex Cloud"
    )
    parser.add_argument(
        "file",
        type=str,
        help="Путь к локальному файлу для загрузки",
    )
    parser.add_argument(
        "-k",
        "--key",
        type=str,
        default=None,
        help="Ключ объекта в S3 (по умолчанию — имя файла)",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Минимальный вывод (только ключ или ошибка)",
    )
    args = parser.parse_args()

    log_level = "WARNING" if args.quiet else CONFIG.log_level
    setup_logging(level=log_level, kafka_log_level=CONFIG.kafka_log_level)

    s3_client = S3Client(
        key_id=CONFIG.key_id,
        key_secret=CONFIG.key_secret,
        endpoint_url=CONFIG.s3_endpoint,
    )

    try:
        key = s3_client.upload_file(
            bucket_name=CONFIG.bucket_name,
            local_path=args.file,
            object_key=args.key,
        )
        if args.quiet:
            print(key)
        else:
            logger.info(f"Готово: s3://{CONFIG.bucket_name}/{key}")
        return 0
    except FileNotFoundError as e:
        logger.error(str(e))
        return 1
    except Exception as e:
        logger.exception("Ошибка загрузки: %s", e)
        return 1
    finally:
        s3_client.close()


if __name__ == "__main__":
    sys.exit(main())
