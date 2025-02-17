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
from pull_from_postgres.films.film_from_db import FilmProducer
from pull_from_postgres.films.film_from_db_by_genre import GenreProducer
from pull_from_postgres.films.film_from_db_by_person import PersonProducer
from pull_from_postgres.genres.genre_from_db import GenresGenreProducer
from push_to_elastic.film_loader_to_elastic import FilmLoader
from push_to_elastic.genre_loader_to_elastic import GenreLoader

POSTGRES_DSN = os.getenv("POSTGRES_DSN")
REDIS_URL = os.getenv("REDIS_URL")
ES_BASE_URL = os.getenv("ES_BASE_URL")


QUEUE_FILM = "film_queue"
QUEUE_GENRES = "genres_queue"
QUEUE_LIMIT = 500


async def main():
    
    pg_pool = await init_postgres(POSTGRES_DSN)
    redis_pool = await init_redis(REDIS_URL)
    es_client = await init_elasticsearch(ES_BASE_URL)

    # producer загружают данные из Postgres в Redis
    producer = FilmProducer(pg_pool=pg_pool,redis_conn=redis_pool,
                            queue_name=QUEUE_FILM, queue_limit=QUEUE_LIMIT,batch_size=500,)
    person_producer = PersonProducer(pg_pool=pg_pool,redis_conn=redis_pool,
                                    queue_name=QUEUE_FILM, queue_limit=QUEUE_LIMIT,
                                    batch_size=500,)
    genre_producer = GenreProducer(pg_pool=pg_pool,redis_conn=redis_pool,
                                  queue_name=QUEUE_FILM, queue_limit=QUEUE_LIMIT,
                                  batch_size=500,)
    genres_producer = GenresGenreProducer(pg_pool=pg_pool,redis_conn=redis_pool,
                                        queue_name=QUEUE_GENRES, queue_limit=QUEUE_LIMIT,
                                        batch_size=500,)

    
    # consumer загружают данные из Redis в Elasticsearch
    film_consumer = FilmLoader(redis_conn=redis_pool, es_client=es_client,
                         batch_size=1000, queue_name=QUEUE_FILM, es_index="movies")
    genres_consumer = GenreLoader(redis_conn=redis_pool, es_client=es_client, batch_size=1000,
                                  queue_name=QUEUE_GENRES,es_index="genres")
    
    try:
        await asyncio.gather(
            producer.run(),
            person_producer.run(),
            genre_producer.run(),
            
            genres_producer.run(),
            
            film_consumer.run(),
            genres_consumer.run(),
        )
        
    finally:
        
        await pg_pool.close()
        await redis_pool.close()
        await es_client.close()


if __name__ == "__main__":    
    asyncio.run(main())