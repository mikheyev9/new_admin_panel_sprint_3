import asyncio
from typing import Any, Tuple
from datetime import datetime, timedelta
import logging
from dataclasses import dataclass


import asyncpg
from redis import asyncio as aioredis
from pydantic import ValidationError

logger = logging.getLogger(__name__)

from extract.sql.film_sql import FETCH_FILMS_BATCH, MAX_LAST_MODIFIED
from transform.models import FilmModel
from utils.backoff import backoff
from utils.waiting import WaitingManager


@dataclass
class FilmProducer:
    """
    Отвечает за:
    - Извлечение данных о Film и связанных записях, которые были изменены с момента последнего извлечения.
    - Сохранение состояния последнего извлечения (время и смещение) в Redis.
    - Сериализацию данных в формат JSON.
    - Помещение сериализованных данных в очередь Redis.
    """
    
    pg_pool: asyncpg.Pool
    redis_conn: aioredis.Redis
    queue_name: str = "film_queue"
    state_key: str = "film_producer_state"
    batch_size: int = 100
    queue_limit: int = 500


    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError))
    async def get_last_state(self) -> Tuple[datetime, int]:
        """
        Извлекает последнее сохраненное состояние обработки данных из Redis.
        
        Состояние включает в себя дату и время последнего изменения (last_modified)
        и смещение (offset) для запроса данных из бд.
        """
        
        state = await self.redis_conn.hgetall(self.state_key)

        last_modified_str = state.get(b"film_last_modified", b"1970-01-01T00:00:00").decode("utf-8")
        last_modified = datetime.fromisoformat(last_modified_str)
        offset = int(state.get(b"film_offset", 0))
        
        logger.debug(f'Получено состояние: last_modified={last_modified}, offset={offset}')
        return last_modified, offset


    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError))
    async def save_state(self, last_modified: datetime, offset:int) -> None:
        """
        Сохраняет текущее состояние обработки данных в Redis.
        """
        
        await self.redis_conn.hset(self.state_key, mapping={
            "film_last_modified": last_modified.isoformat(),
            "film_offset": offset
        })
        
        logger.debug(f'Сохранено состояние: last_modified={last_modified}, offset={offset}')


    @backoff(exceptions=(asyncpg.PostgresError,))
    async def fetch_data_batch(self, last_modified: datetime, offset: int) -> list[asyncpg.Record]:
        
        async with self.pg_pool.acquire() as connection:
            query = FETCH_FILMS_BATCH
            params = (last_modified, self.batch_size, offset)

            rows = await connection.fetch(query, *params)
            
            logger.debug(f'Извлечено {len(rows)} записей из базы данных с last_modified={last_modified}, offset={offset}')
            return rows
    
    
    @backoff(exceptions=(asyncpg.PostgresError,))
    async def fetch_max_last_modified(self) -> datetime:
        
        async with self.pg_pool.acquire() as connection:
            
            query = MAX_LAST_MODIFIED
            max_last_modified = await connection.fetchval(query)

            return max_last_modified


    @staticmethod
    def serialize_data(record: asyncpg.Record) -> str:
        """
        record:
            <Record 
                id=UUID('3d825f60-9fff-4dfe-b294-1a45fa1e115d') 
                modified=datetime.datetime(2021, 6, 16, 20, 14, 9, 221855) 
                title='Star Wars: Episode IV - A New Hope' 
                description='The Imperial Forces, under orders from cruel Darth Vader, hold Princess Leia hostage...' 
                imdb_rating=8.6 
                genres=['Action', 'Adventure', 'Fantasy', 'Sci-Fi'] 
                actors='[{"id": "26e83050-29ef-4163-a99d-b546cac208f8", "name": "Mark Hamill"}, ...]' 
                directors='[{"id": "a5a8f573-3cee-4ccc-8a2b-91cb9f55250a", "name": "George Lucas"}]' 
                writers='[{"id": "a5a8f573-3cee-4ccc-8a2b-91cb9f55250a", "name": "George Lucas"}]'
            >
        """
        
        try:
            film_data = FilmModel(**dict(record))
            return film_data.model_dump_json()
        except ValidationError as e:
            logger.error(f"Ошибка валидации данных: {e} {record}")
            raise


    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError))
    async def push_to_queue(self, data_batch: list[Any]):
        wait = WaitingManager(start_time=1.0, max_time=60.0, factor=2.0)

        while True:
            queue_length = await self.redis_conn.llen(self.queue_name)

            if queue_length + len(data_batch) <= self.queue_limit:

                logger.debug(f'Добавление {len(data_batch)} элементов в очередь {self.queue_name}')
                await self.redis_conn.rpush(self.queue_name, *data_batch)
                break

            logger.debug(f'Очередь {self.queue_name} заполнена. Текущий размер: {queue_length}. Ожидание...')
            await wait.wait()


    async def run(self):
        """
        Основной цикл:
          1. Читает текущее состояние (last_modified, offset).
          2. Берёт изменённые фильмы.
          3. Находит все связанные данные.
          4. Сериализует фильмы и отправляет в Redis.
          5. Обновляет offset.
        """
        wait = WaitingManager(start_time=1.0, max_time=60.0, factor=2.0)

        while True:
            
            last_modified, offset = await self.get_last_state()

            data_batch = await self.fetch_data_batch(last_modified, offset)

            if not data_batch:
                
                if offset > 0:
                    offset = 0
                    
                    max_last_modified = await self.fetch_max_last_modified()
                    new_last_modified = max_last_modified + timedelta(seconds=1)
                    
                    await self.save_state(new_last_modified, offset)
                    
                logger.debug(f'Новые данные отсутствуют. Сброс offset до 0 и обновление last_modified до наибольшего в БД')
                await wait.wait()

            else:
                
                serialized_batch = [self.serialize_data(row) for row in data_batch]
                logger.debug(f'Успешно сериализовано {len(serialized_batch)} элементов.')

                await self.push_to_queue(serialized_batch)
                
                offset += len(data_batch)
                await self.save_state(last_modified, offset)
                
                wait.reset()

