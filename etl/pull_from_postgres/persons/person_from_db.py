import asyncio
from dataclasses import dataclass
from typing import List, AsyncGenerator

import asyncpg

from utils.backoff import backoff
from pull_from_postgres.base import BaseProducer
from model.models import PersonDBModel
from pull_from_postgres.sql.person_sql import FETCH_PERSONS_BATCH

@dataclass
class PersonsOnlyProducer(BaseProducer):
    """Класс для загрузки персон из PostgreSQL в Redis."""
    
    state_key: str = "persons_index_state"
    last_modified_field: str = "persons_index_last_modified"
    last_id_field: str = "persons_index_last_id"

    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError))
    async def fetch_main_batch(self, last_modified, last_id) -> List[asyncpg.Record]:
        async with self.pg_pool.acquire() as connection:
            rows = await connection.fetch(
                FETCH_PERSONS_BATCH,
                last_modified,
                last_id,
                self.batch_size)
            if rows:
                last_modified = rows[-1]['modified']
                last_id = rows[-1]['id']
            return rows, last_modified, last_id

    async def fetch_related_data(self, main_batch: List[asyncpg.Record]) -> AsyncGenerator[List[asyncpg.Record], None]:
        # Для персон связанных данных нет, возвращаем сразу батч.
        yield main_batch

    def serialize_record(self, record: asyncpg.Record) -> str:
        """Сериализует запись в JSON-формат для Redis."""
        person = PersonDBModel(**dict(record))
        return person.model_dump_json()
