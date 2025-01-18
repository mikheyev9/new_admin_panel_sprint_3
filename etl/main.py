import asyncio
import logging
import os

from dotenv import load_dotenv
load_dotenv()

from log.logger import setup_global_logger
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
setup_global_logger(level=getattr(logging, LOG_LEVEL, logging.INFO))


from db.driver import init_postgres, init_redis
from extract.film import FilmProducer
from extract.genre import GenreProducer
from extract.person import PersonProducer
from load.film_loader import FilmConsumer

POSTGRES_DSN = os.getenv("POSTGRES_DSN")
REDIS_URL = os.getenv("REDIS_URL")
ES_BASE_URL = os.getenv("ES_BASE_URL")


async def main():
    pg_pool = await init_postgres(POSTGRES_DSN)
    redis_pool = await init_redis(REDIS_URL)

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
    
    consumer = FilmConsumer(
        redis_conn=redis_pool,
        batch_size=1000,
        es_base_url=ES_BASE_URL
    )

    await asyncio.gather(
        producer.run(),
        consumer.run(),
        person_producer.run(),
        genre_producer.run(),
    )


if __name__ == "__main__":    
    asyncio.run(main())