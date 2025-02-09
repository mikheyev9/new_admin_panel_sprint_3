import asyncio
import logging
import os
from signal import SIGTERM, SIGINT, SIGHUP

from dotenv import load_dotenv
load_dotenv()

from log.logger import setup_global_logger
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
setup_global_logger(level=getattr(logging, LOG_LEVEL, logging.INFO))

logger = logging.getLogger(__name__)

from db.driver import (init_postgres,
                       init_redis,
                       init_elasticsearch)
from extract.film import FilmProducer
from extract.genre import GenreProducer
from extract.person import PersonProducer
from load.film_loader import FilmLoader

POSTGRES_DSN = os.getenv("POSTGRES_DSN")
REDIS_URL = os.getenv("REDIS_URL")
ES_BASE_URL = os.getenv("ES_BASE_URL")


async def run_with_recovery(coro_func, name):
    while True:
        try:
            await coro_func()
        except Exception as e:
            logging.error(f"[ main ] Process {name} failed with error: {e}")
            await asyncio.sleep(5)
            logging.info(f"[ main ] Restarting {name}...")
            
            
async def shutdown(signal, loop, shutdown_event):
    """Корректное завершение приложения"""
    logger.info(f"[ main ] Получен сигнал завершения {signal.name}...")
    shutdown_event.set()  # Устанавливаем событие завершения
    
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    
    logger.info(f"[ main ] Отмена {len(tasks)} задач...")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()


async def main():
    
    pg_pool = await init_postgres(POSTGRES_DSN)
    redis_pool = await init_redis(REDIS_URL)
    es_client = await init_elasticsearch(ES_BASE_URL)

    producer = FilmProducer(
        pg_pool=pg_pool,
        redis_conn=redis_pool,
        batch_size=500,
        queue_limit=500,
    )
    person_producer = PersonProducer(
        pg_pool=pg_pool,
        redis_conn=redis_pool,
        batch_size=10,
        queue_limit=500,
    )
    genre_producer = GenreProducer(
        pg_pool=pg_pool,
        redis_conn=redis_pool,
        batch_size=10,
        queue_limit=500,
    )
    
    # Создаем загрузчик фильмов, который берет данные из очереди Redis и загружает их в Elasticsearch
    consumer = FilmLoader(
        redis_conn=redis_pool,
        es_client=es_client,
        batch_size=1000,
        queue_name="film_queue",
        es_index="movies"   
    )
    
    shutdown_event = asyncio.Event()
     # Настраиваем обработчики сигналов
    loop = asyncio.get_running_loop()
    signals = (SIGHUP, SIGTERM, SIGINT)
    for sig in signals:
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(
                shutdown(s, loop, shutdown_event)
            )
        )

    try:
        # Запускаем все процессы с механизмом восстановления
        await asyncio.gather(
            run_with_recovery(producer.run, "FilmProducer"),
            run_with_recovery(consumer.run, "FilmLoader"),
            run_with_recovery(person_producer.run, "PersonProducer"),
            run_with_recovery(genre_producer.run, "GenreProducer"),
            shutdown_event.wait()
        )
        
    finally:
        
        await pg_pool.close()
        await redis_pool.close()
        await es_client.close()
        loop.stop()


if __name__ == "__main__":    
    asyncio.run(main())