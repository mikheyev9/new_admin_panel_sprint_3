import asyncio
import logging
import random
from functools import wraps
from typing import Callable, Tuple, Type

logger = logging.getLogger(__name__)


def backoff(
        exceptions: Tuple[Type[BaseException], ...] = (Exception,),
        start_sleep_time: float = 2.0,
        factor: float = 2.0,
        border_sleep_time: float = 30.0,
        max_attempts: int = 10,
        jitter: bool = True
):
    """
    Декоратор для повторного выполнения асинхронной функции с экспоненциальным ростом задержки.

    :param exceptions: Исключения, при которых необходимо повторить выполнение
    :param start_sleep_time: Начальное время ожидания перед повторной попыткой
    :param factor: Множитель увеличения времени ожидания
    :param border_sleep_time: Максимальное время ожидания между попытками
    :param max_attempts: Максимальное количество попыток
    :param jitter: Добавлять ли случайный разброс к задержке
    """

    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            sleep_time = start_sleep_time

            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt >= max_attempts:
                        logger.error(f"Функция {func.__name__} достигла максимального числа попыток ({max_attempts}): {e}")
                        raise e

                    wait_time = min(sleep_time, border_sleep_time)
                    if jitter:
                        wait_time += random.uniform(0, wait_time * 0.1)

                    logger.warning(
                        f"Ошибка {e.__class__.__name__}: {e}. "
                        f"Попытка {attempt}/{max_attempts}. "
                        f"Повтор через {wait_time:.2f} секунд."
                    )

                    await asyncio.sleep(wait_time)
                    sleep_time *= factor

        return wrapper

    return decorator
