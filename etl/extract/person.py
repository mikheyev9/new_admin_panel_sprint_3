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
from extract.sql.person_sql import (FETCH_PERSONS_BATCH,
                                  FETCH_FILMS_BY_PERSON,
                                  MAX_LAST_MODIFIED_PERSON)

logger = logging.getLogger(__name__)


@dataclass
class PersonProducer:
    """
    Аналог FilmProducer, но фокусируется на изменениях в таблице person:
      1. Следит, кто изменился.
      2. Для каждого изменившегося person вытягивает все связанные фильмы.
      3. Отправляет информацию по этим фильмам в Redis.
    """
    pg_pool: asyncpg.Pool
    redis_conn: aioredis.Redis

    queue_name: str = "film_queue"

    state_key: str = "person_producer_state"

    batch_size: int = 100
    queue_limit: int = 500


    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError))
    async def get_last_state(self) -> Tuple[datetime, int]:
        """
        Извлекает последнее сохраненное состояние (дата+время, offset) из Redis.
        Если состояние отсутствует, использует значение max_last_modified.
        """
        
        state = await self.redis_conn.hgetall(self.state_key)

        if not state:
            
            max_last_modified = await self.fetch_max_last_modified() + timedelta(seconds=5)
            last_modified = max_last_modified
            offset = 0
            await self.save_state(max_last_modified, offset)
            
        else:
            
            last_modified_str = state.get(b"person_last_modified")
            
            if last_modified_str:
                
                last_modified = datetime.fromisoformat(last_modified_str.decode("utf-8"))
                
            else:
                
                last_modified = max_last_modified
            
            offset = int(state.get(b"person_offset", 0))

        logger.debug(f'[PersonProducer] Получено состояние: last_modified={last_modified}, offset={offset}')
        
        return last_modified, offset


    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError))
    async def save_state(self, last_modified: datetime, offset: int) -> None:
        """
        Сохраняет текущее состояние (дата+время + offset) в Redis.
        """
        
        await self.redis_conn.hset(self.state_key, mapping={
            "person_last_modified": last_modified.isoformat(),
            "person_offset": offset
        })
        
        logger.debug(f'[PersonProducer] Сохранено состояние: last_modified={last_modified}, offset={offset}')


    @backoff(exceptions=(asyncpg.PostgresError,))
    async def fetch_persons_batch(self, last_modified: datetime, offset: int) -> list[asyncpg.Record]:
        """
        Извлекаем «пачку» изменившихся Person.
        """
        
        async with self.pg_pool.acquire() as connection:
            
            rows = await connection.fetch(FETCH_PERSONS_BATCH, last_modified, self.batch_size, offset)
            logger.debug(
                f'[PersonProducer] Извлечено {len(rows)} person-записей из БД '
                f'(last_modified={last_modified}, offset={offset})'
            )
            return rows


    @backoff(exceptions=(asyncpg.PostgresError,))
    async def fetch_films_by_person(self, person_id: str) -> list[asyncpg.Record]:
        """
        Извлекаем все фильмы, связанные с данным Person (через person_film_work).
        """
        
        async with self.pg_pool.acquire() as connection:
            
            rows = await connection.fetch(FETCH_FILMS_BY_PERSON, person_id)
            
            return rows


    @backoff(exceptions=(asyncpg.PostgresError,))
    async def fetch_max_last_modified(self) -> datetime:
        """
        Аналогичная логика, как в FilmProducer. 
        Берём максимальную дату изменения (modified) из таблицы person,
        чтобы перезаписать `last_modified`, если новых данных нет.
        """
        async with self.pg_pool.acquire() as connection:
            max_last_modified = await connection.fetchval(MAX_LAST_MODIFIED_PERSON)
            return max_last_modified


    @staticmethod
    def serialize_data(record: asyncpg.Record) -> str:
        """
        Сериализуем запись о фильме (FilmWork) в JSON (используя нашу FilmModel).
        """
        
        try:
            
            film_data = FilmModel(**dict(record))
            
            return film_data.model_dump_json()
        
        except ValidationError as e:
        
            logger.error(f"[PersonProducer] Ошибка валидации данных: {e} {record}")
            raise


    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError))
    async def push_to_queue(self, data_batch: list[str]):
        """
        Добавляем сериализованные фильмы в Redis-очередь. Аналогично FilmProducer.
        """
        
        wait = WaitingManager(start_time=1.0, max_time=60.0, factor=2.0)

        while True:
            
            queue_length = await self.redis_conn.llen(self.queue_name)
            
            if queue_length + len(data_batch) <= self.queue_limit:
                
                logger.debug(f'[PersonProducer] Добавление {len(data_batch)} элементов в очередь {self.queue_name}')
                await self.redis_conn.rpush(self.queue_name, *data_batch)
                break
            
            else:
                
                logger.debug(
                    f'[PersonProducer] Очередь {self.queue_name} переполнена (размер={queue_length}). Ожидание...'
                )
                await wait.wait()


    async def run(self):
        """
        Основной цикл:
          1. Читает текущее состояние (last_modified, offset).
          2. Берёт изменённые person.
          3. Находит все связанные фильмы.
          4. Находит все связанные данные по фильмам.
          5. Сериализует фильмы и отправляет в Redis.
          6. Обновляет offset.
        """
        wait = WaitingManager(start_time=1.0, max_time=60.0, factor=2.0)

        while True:
            
            last_modified, offset = await self.get_last_state()
            
            person_batch = await self.fetch_persons_batch(last_modified, offset)

            if not person_batch:
                
                if offset > 0:
                    offset = 0
                    
                    max_last_modified = await self.fetch_max_last_modified()
                    new_last_modified = max_last_modified + timedelta(seconds=1)
                    
                    await self.save_state(new_last_modified, offset)

                logger.debug('[PersonProducer] Новые данные отсутствуют. Сброс offset до 0.')
                await wait.wait()
                
            else:
                
                all_films_data = []
                for person_record in person_batch:
                    
                    person_id = str(person_record["id"])
                    
                    films = await self.fetch_films_by_person(person_id)
                    
                    if not films:
                        continue
                    
                    all_films_data = [self.serialize_data(fw) for fw in films]
                    

                if all_films_data:
                    
                    await self.push_to_queue(all_films_data)

                
                offset += len(person_batch)
            
                await self.save_state(last_modified, offset)
                
                wait.reset()

