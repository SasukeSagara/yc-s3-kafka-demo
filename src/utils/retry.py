"""Утилиты для повторных попыток выполнения операций"""

import logging
import time
from functools import wraps
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


def retry(
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    exceptions: tuple[type[Exception], ...] = (Exception,),
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Декоратор для повторных попыток выполнения функции

    Args:
        max_attempts: Максимальное количество попыток
        delay: Начальная задержка между попытками в секундах
        backoff: Множитель для увеличения задержки
        exceptions: Кортеж исключений, при которых нужно повторять попытку

    Returns:
        Декорированная функция
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            current_delay = delay
            last_exception: Exception | None = None

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_attempts:
                        logger.warning(
                            f"Попытка {attempt}/{max_attempts} не удалась для {func.__name__}: {e}. "
                            f"Повтор через {current_delay:.1f}с."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"Все {max_attempts} попыток не удались для {func.__name__}: {e}"
                        )

            if last_exception:
                raise last_exception

            # Этот код не должен выполняться, но нужен для типизации
            raise RuntimeError("Неожиданная ошибка в retry декораторе")

        return wrapper

    return decorator
