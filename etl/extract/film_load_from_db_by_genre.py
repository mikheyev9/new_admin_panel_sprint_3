import logging
from typing import List, AsyncGenerator
from dataclasses import dataclass
import asyncpg
import json
from pydantic import ValidationError
from datetime import datetime

from model.models import FilmModel
from pull_from_postgres.sql.genre_sql import (FETCH_GENRES_BATCH,
                                   FETCH_FILMS_BY_GENRE)
from pull_from_postgres.base import BaseProducer

logger = logging.getLogger(__name__)

@dataclass
class GenreProducer(BaseProducer):
    """
    Producer для жанров:
      - fetch_main_batch: возвращает пачку изменившихся жанров.
      - fetch_related_data: по каждому жанру достаём все связанные фильмы.
    """
    queue_name: str = "film_queue"
    state_key: str = "genre_producer_state"
    last_modified_field: str = "genre_last_modified"
    last_id_field: str = "genre_last_id"

    async def fetch_main_batch(self, last_modified, last_id) -> List[asyncpg.Record]:
        async with self.pg_pool.acquire() as connection:
            rows = await connection.fetch(FETCH_GENRES_BATCH,
                                          last_modified,
                                          last_id,
                                          self.batch_size)

            if rows:
                last_modified = rows[-1]['modified']
                last_id = rows[-1]['id']
            
            return rows, last_modified, last_id


    async def fetch_related_data(self, main_batch: List[asyncpg.Record]) -> AsyncGenerator[List[asyncpg.Record], None]:
        """
        На основе списка жанров достаём фильмы порциями через Keyset Pagination.
        """
        
        async with self.pg_pool.acquire() as connection:
            for genre_record in main_batch:
                genre_id = str(genre_record["id"])
                last_id = None

                while True:
                    batch = await connection.fetch(
                        FETCH_FILMS_BY_GENRE,
                        genre_id,
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
