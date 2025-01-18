import asyncio
import logging
import random
from functools import wraps
from typing import Callable, Tuple, Type

logger = logging.getLogger(__name__)


def backoff(
        start_sleep_time: float = 2,
        factor: float = 2,
        border_sleep_time: float = 30,
        exceptions: Tuple[Type[BaseException], ...] = (Exception,),
        max_attempts: int = 100
):
    """
    Декоратор для повторного выполнения функции через некоторое время, если возникла ошибка.
    Добавляет экспоненциальный рост времени повтора с ограничением и jitter.

    :param start_sleep_time: начальное время ожидания
    :param factor: во сколько раз увеличивать время ожидания
    :param border_sleep_time: максимальное время ожидания
    :param exceptions: типы ошибок, для которых требуется повтор
    :param max_attempts: максимальное количество попыток
    """

    def func_wrapper(func: Callable):
        @wraps(func)
        async def inner(*args, **kwargs):
            attempts = 0
            sleep_time = start_sleep_time

            while attempts < max_attempts:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    attempts += 1
                    jitter = random.uniform(0, sleep_time * 0.1)
                    wait_time = min(sleep_time + jitter, border_sleep_time)

                    logger.warning(
                        f"Ошибка {e.__class__.__name__}: {e}. "
                        f"Попытка {attempts}/{max_attempts}. "
                        f"Повтор через {wait_time:.2f} секунд."
                    )

                    if attempts >= max_attempts:
                        logger.error(f"Превышено количество попыток для функции {func.__name__}.")
                        raise e

                    await asyncio.sleep(wait_time)
                    sleep_time *= factor

        return inner

    return func_wrapper
