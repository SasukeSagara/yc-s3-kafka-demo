"""Настройка логирования"""

import logging
import sys
from typing import Optional


def setup_logging(
    level: str = "INFO",
    format_string: Optional[str] = None,
    kafka_log_level: Optional[str] = None,
) -> None:
    """
    Настраивает логирование для приложения

    Args:
        level: Уровень логирования (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_string: Формат строки логирования (опционально)
        kafka_log_level: Уровень для логгеров kafka.* (kafka.conn и др.). Если None — используется level.
    """
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format=format_string,
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    if kafka_log_level is not None:
        logging.getLogger("kafka").setLevel(getattr(logging, kafka_log_level.upper()))
