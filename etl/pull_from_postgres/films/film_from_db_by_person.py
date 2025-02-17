import asyncio
import logging
from typing import List, AsyncGenerator
from dataclasses import dataclass

import asyncpg
from pydantic import ValidationError

from utils.backoff import backoff
from model.models import FilmModel
from pull_from_postgres.sql.person_sql import (FETCH_PERSONS_BATCH,
                                    FETCH_FILMS_BY_PERSON)
from pull_from_postgres.base import BaseProducer

logger = logging.getLogger(__name__)

@dataclass
class PersonProducer(BaseProducer):
    """
    Producer для персон (actors, writers, directors):
      - fetch_main_batch: возвращает пачку изменившихся персон.
      - fetch_related_data: по каждой персоне достаём все связанные фильмы.
    """
    state_key: str = "person_producer_state"
    last_modified_field: str = "person_last_modified"
    last_id_field: str = "person_last_id"


    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError, asyncpg.exceptions.PostgresConnectionError))
    async def fetch_main_batch(self, last_modified, last_id) -> List[asyncpg.Record]:
        async with self.pg_pool.acquire() as connection:
            rows = await connection.fetch(FETCH_PERSONS_BATCH,
                                          last_modified,
                                          last_id,
                                          self.batch_size)

            if rows:
                last_modified = rows[-1]['modified']
                last_id = rows[-1]['id']
            
            return rows, last_modified, last_id


    async def fetch_related_data(self, main_batch: List[asyncpg.Record]) -> AsyncGenerator[List[asyncpg.Record], None]:
        """
        Загружаем фильмы по персонам через Keyset Pagination.
        Вместо OFFSET используем `fw.id` (или `fw.modified`).
        """
        async with self.pg_pool.acquire() as connection:
            for person_record in main_batch:
                person_id = str(person_record["id"])
                last_id = None

                while True:
                    batch = await connection.fetch(
                        FETCH_FILMS_BY_PERSON,
                        person_id,
                        self.queue_limit,
                        last_id
                    )
                    if not batch:
                        break

                    yield batch

                    last_id = batch[-1]["id"]  


    def serialize_record(self, record: asyncpg.Record) -> str:
        try:
            film_data = FilmModel(**dict(record))
            return film_data.model_dump_json()
        except ValidationError as e:
            logger.error(f"[{self.__class__.__name__}] Ошибка валидации данных: {e} {record}")
