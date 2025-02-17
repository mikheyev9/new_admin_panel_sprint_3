import logging
from datetime import datetime
from typing import List, AsyncGenerator
from dataclasses import dataclass
import asyncpg

from pydantic import ValidationError

from model.models import FilmModel
from pull_from_postgres.sql.film_sql import FETCH_FILMS_BATCH
from pull_from_postgres.base import BaseProducer

logger = logging.getLogger(__name__)


@dataclass
class FilmProducer(BaseProducer):
    """
    Producer для фильмов.
    Не требует дополнительного «fetch_related_data».
    """
    
    queue_name: str
    state_key: str = "film_producer_state"
    last_modified_field: str = "film_last_modified"
    last_id_field: str = "film_last_id"

    async def fetch_main_batch(self, last_modified, last_id) -> List[asyncpg.Record]:
        async with self.pg_pool.acquire() as connection:
            rows = await connection.fetch(FETCH_FILMS_BATCH,
                                          last_modified,
                                          last_id,
                                          self.batch_size)

            if rows:
                last_modified = rows[-1]['modified']
                last_id = rows[-1]['id']
            
            return rows, last_modified, last_id


    async def fetch_related_data(self, main_batch: List[asyncpg.Record]) -> AsyncGenerator[List[asyncpg.Record], None]:
        yield main_batch


    def serialize_record(self, record: asyncpg.Record) -> str:
        try:
            film_data = FilmModel(**dict(record))
            return film_data.model_dump_json()
        except ValidationError as e:
            logger.error(f"[{self.__class__.__name__}] Ошибка валидации данных: {e} {record}")
