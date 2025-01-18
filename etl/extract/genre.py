import asyncio
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Tuple, Any

import asyncpg
from redis import asyncio as aioredis
from pydantic import ValidationError

from utils.backoff import backoff
from utils.waiting import WaitingManager

from transform.models import FilmModel
from extract.sql.genre_sql import (FETCH_GENRES_BATCH,
                                 FETCH_FILMS_BY_GENRE,
                                 MAX_LAST_MODIFIED_GENRE)

logger = logging.getLogger(__name__)


@dataclass
class GenreProducer:
    """
    Аналог FilmProducer, но следит за изменениями в таблице genre:
      1. Отслеживает изменённые жанры.
      2. Для каждого изменившегося жанра получает все фильмы.
      3. Отправляет обновлённые фильмы в Redis.
    """
    pg_pool: asyncpg.Pool
    redis_conn: aioredis.Redis

    queue_name: str = "film_queue"
    state_key: str = "genre_producer_state"

    batch_size: int = 100
    queue_limit: int = 500


    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError))
    async def get_last_state(self) -> Tuple[datetime, int]:
        """
        Извлекает последнее сохранённое состояние (дата+время, offset) из Redis.
        """
        state = await self.redis_conn.hgetall(self.state_key)

        if not state:
            
            max_last_modified = await self.fetch_max_last_modified() + timedelta(seconds=5)
            last_modified = max_last_modified
            offset = 0
            
            await self.save_state(max_last_modified, offset)
            
        else:
            
            last_modified_str = state.get(b"genre_last_modified")
            
            if last_modified_str:
                
                last_modified = datetime.fromisoformat(last_modified_str.decode("utf-8"))
                
            else:
                
                last_modified = max_last_modified
                
            offset = int(state.get(b"genre_offset", 0))

        logger.debug(f'[GenreProducer] Получено состояние: last_modified={last_modified}, offset={offset}')
        return last_modified, offset


    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError))
    async def save_state(self, last_modified: datetime, offset: int) -> None:
        """
        Сохраняет текущее состояние (дата+время + offset) в Redis.
        """
        await self.redis_conn.hset(self.state_key, mapping={
            "genre_last_modified": last_modified.isoformat(),
            "genre_offset": offset
        })
        logger.debug(f'[GenreProducer] Сохранено состояние: last_modified={last_modified}, offset={offset}')


    @backoff(exceptions=(asyncpg.PostgresError,))
    async def fetch_genres_batch(self, last_modified: datetime, offset: int) -> list[asyncpg.Record]:
        """
        Извлекаем изменённые жанры.
        """
        async with self.pg_pool.acquire() as connection:
            rows = await connection.fetch(FETCH_GENRES_BATCH, last_modified, self.batch_size, offset)
            logger.debug(
                f'[GenreProducer] Извлечено {len(rows)} genre-записей из БД '
                f'(last_modified={last_modified}, offset={offset})'
            )
            return rows


    @backoff(exceptions=(asyncpg.PostgresError,))
    async def fetch_films_by_genre(self, genre_id: str) -> list[asyncpg.Record]:
        """
        Извлекаем все фильмы, связанные с данным жанром.
        """
        async with self.pg_pool.acquire() as connection:
            rows = await connection.fetch(FETCH_FILMS_BY_GENRE, genre_id)
            return rows


    @backoff(exceptions=(asyncpg.PostgresError,))
    async def fetch_max_last_modified(self) -> datetime:
        """
        Берём максимальную дату изменения (modified) из таблицы genre.
        """
        async with self.pg_pool.acquire() as connection:
            max_last_modified = await connection.fetchval(MAX_LAST_MODIFIED_GENRE)
            return max_last_modified


    @staticmethod
    def serialize_data(record: asyncpg.Record) -> str:
        """
        Сериализуем запись о фильме (FilmWork) в JSON.
        """
        try:
            film_data = FilmModel(**dict(record))
            return film_data.model_dump_json()
        except ValidationError as e:
            logger.error(f"[GenreProducer] Ошибка валидации данных: {e} {record}")
            raise

    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError))
    async def push_to_queue(self, data_batch: list[str]):
        """
        Добавляем сериализованные фильмы в Redis-очередь.
        """
        wait = WaitingManager(start_time=1.0, max_time=60.0, factor=2.0)

        while True:
            queue_length = await self.redis_conn.llen(self.queue_name)
            if queue_length + len(data_batch) <= self.queue_limit:
                logger.debug(f'[GenreProducer] Добавление {len(data_batch)} элементов в очередь {self.queue_name}')
                await self.redis_conn.rpush(self.queue_name, *data_batch)
                break
            logger.debug(
                f'[GenreProducer] Очередь {self.queue_name} переполнена (размер={queue_length}). Ожидание...'
            )
            await wait.wait()

    async def run(self):
        """
        Основной цикл:
          1. Читает текущее состояние (last_modified, offset).
          2. Берёт изменённые жанры.
          3. Находит все связанные фильмы.
          4. Находит все связанные данные по фильмам.
          5. Сериализует фильмы и отправляет в Redis.
          6. Обновляет offset.
        """
        wait = WaitingManager(start_time=1.0, max_time=60.0, factor=2.0)

        while True:
            last_modified, offset = await self.get_last_state()
            genre_batch = await self.fetch_genres_batch(last_modified, offset)

            if not genre_batch:
                if offset > 0:
                    offset = 0
                    max_last_modified = await self.fetch_max_last_modified()
                    new_last_modified = max_last_modified + timedelta(seconds=1)
                    await self.save_state(new_last_modified, offset)

                logger.debug('[GenreProducer] Новые данные отсутствуют. Сброс offset до 0.')
                await wait.wait()

            else:
                all_films_data = []
                for genre_record in genre_batch:
                    genre_id = str(genre_record["id"])
                    films = await self.fetch_films_by_genre(genre_id)
                    if not films:
                        continue
                    all_films_data.extend([self.serialize_data(fw) for fw in films])

                if all_films_data:
                    await self.push_to_queue(all_films_data)

                offset += len(genre_batch)
                await self.save_state(last_modified, offset)

                wait.reset()
