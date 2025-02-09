import logging
import asyncio

import asyncpg
from redis import asyncio as aioredis
from redis.exceptions import ConnectionError
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError

from utils.backoff import backoff


logger = logging.getLogger(__name__)


@backoff(start_sleep_time=2, border_sleep_time=30,
         exceptions=(asyncpg.PostgresError,), max_attempts=100)
async def init_postgres(dsn: str) -> asyncpg.Pool:
    """
    Инициализирует подключение к PostgreSQL и возвращает пул соединений.
    """
    try:
        pool = await asyncpg.create_pool(dsn)
        logger.info("Подключение к PostgreSQL установлено.")
        return pool
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"Ошибка подключения к PostgreSQL: {e}")
        raise


@backoff(start_sleep_time=2, border_sleep_time=30,
         exceptions=(ConnectionError, asyncio.TimeoutError), max_attempts=100)
async def init_redis(redis_url: str) -> aioredis.Redis:
    """
    Инициализирует подключение к Redis и проверяет доступность.
    """
    try:
        redis = aioredis.Redis.from_url(redis_url)
        await redis.ping()
        logger.info("Подключение к Redis установлено.")
        return redis
    except ConnectionError as e:
        logger.error(f"Ошибка подключения к Redis: {e}")
        raise
    except Exception as e:
        logger.error(f"Неизвестная ошибка при подключении к Redis: {e}")
        raise
    

@backoff(start_sleep_time=2, border_sleep_time=30,
         exceptions=(ESConnectionError, asyncio.TimeoutError), max_attempts=10)
async def init_elasticsearch(es_url: str) -> AsyncElasticsearch:
    """
    Инициализирует подключение к Elasticsearch и проверяет доступность.
    """
    try:
        es_client = AsyncElasticsearch([es_url])
        await es_client.info()
        logger.info("Подключение к Elasticsearch установлено.")
        return es_client
    except ESConnectionError as e:
        logger.error(f"Ошибка подключения к Elasticsearch: {e}")
        raise
    except Exception as e:
        logger.error(f"Неизвестная ошибка при подключении к Elasticsearch: {e}")
        raise