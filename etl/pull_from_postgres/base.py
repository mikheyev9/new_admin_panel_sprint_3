import asyncio
import logging
from datetime import datetime, timedelta
from typing import Tuple, Any, List, AsyncGenerator
from abc import ABC, abstractmethod
from dataclasses import dataclass

import asyncpg
from redis import asyncio as aioredis

from utils.backoff import backoff
from utils.waiting import WaitingManager

logger = logging.getLogger(__name__)


@dataclass
class BaseProducer(ABC):
    """
    Базовый абстрактный класс для «producer»-классов,
    которые читают данные из Postgres и складывают в Redis.
    """
    
    pg_pool: asyncpg.Pool
    redis_conn: aioredis.Redis

    queue_name: str
    queue_limit: int
    batch_size: int
    state_key: str
    last_modified_field: str
    last_id_field: str
    default_modified: datetime = datetime(1800, 1, 1)
    
    def __post_init__(self):
        self.notify_task = asyncio.create_task(self.notify_on_space())
        self.wait_for_timer = WaitingManager(start_time=5.0, max_time=60.0, factor=2.0)
        self.wait_redis = asyncio.Event()
            

    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError))
    async def get_last_state(self) -> Tuple[datetime, str|None]:
        """
        Читает текущее состояние (last_modified, offset, last_id) из Redis.
        Если его нет, сохраняет и возвращает default_modified и 0.
        """
        
        state = await self.redis_conn.hgetall(self.state_key)

        if not state:
            last_modified, last_id = self.default_modified, None
        else:
            last_modified_str = state.get(self.last_modified_field.encode("utf-8"))
            last_modified = datetime.fromisoformat(last_modified_str.decode("utf-8"))

            last_id_str = state.get(self.last_id_field.encode("utf-8"))
            last_id = last_id_str.decode("utf-8") if last_id_str else None
            
        return last_modified, last_id


    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError))
    async def save_state(self, last_modified: datetime, last_id: int) -> None: # TODO last_id<class 'asyncpg.pgproto.pgproto.UUID'
        """
        Сохраняет дату проследней просмотренной записи в Postgres.
        Сохраняет id последней просмотренной записи в Postgres.
        """
        #print(last_modified, last_id, type(last_id), "!!!!!!!!1")
        await self.redis_conn.hset(
            self.state_key,
            mapping={
                self.last_modified_field: last_modified.isoformat(),
                self.last_id_field: str(last_id)
            }
        )


    @abstractmethod
    async def fetch_main_batch(self,
                               last_modified: datetime,
                               last_id: str|None) -> List[asyncpg.Record]:
        """
        Извлекает «первичную» пачку данных из Postgres.
        """
        pass


    @abstractmethod
    async def fetch_related_data(self, main_batch: List[asyncpg.Record]) -> AsyncGenerator[List[asyncpg.Record], None]:
        """
        Извлекает связанные данные порциями, если нужно.
        Возвращает асинхронный генератор, который выдает данные частями,
        не превышающими размер очереди.
        """
        pass


    @abstractmethod
    def serialize_record(self, record: asyncpg.Record) -> str:
        """
        Преобразует одну запись (Record) в JSON-строку для отправки в Redis.
        Часто это делается через Pydantic-модель (FilmModel, PersonModel, ...).
        """
        pass


    async def notify_on_space(self):
        """ Подписка на освобождение места в очереди """
        
        pubsub = self.redis_conn.pubsub()
        await pubsub.subscribe(f"{self.queue_name}:space")

        async for message in pubsub.listen():
            if message["type"] == "message":
                logger.info(f"[{self.queue_name}] Очередь освободилась.")
                self.wait_redis.set()  # Разрешаем `push_to_queue` продолжить работу


    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError))
    async def push_to_queue(self, data_batch: List[str]) -> None:
        """
        Добавляет сериализованные данные в Redis-очередь, 
        учитывая допустимый лимит queue_limit.
        """
        while data_batch:
            async with self.redis_conn.pipeline() as pipe:
                pipe.llen(self.queue_name)
                results = await pipe.execute()
            
            queue_length = results[0]
            capacity = self.queue_limit - queue_length

            if capacity <= 0:
                logger.info(f"[{self.queue_name}] Очередь переполнена ({queue_length}), жду освобождения...")
                self.wait_redis.clear()  # Сбрасываем флаг ожидания
                await self.wait_redis.wait()  # Ждём, пока очередь освободится
                continue

            chunk = data_batch[:capacity]
            data_batch = data_batch[capacity:]

            async with self.redis_conn.pipeline() as pipe:
                pipe.rpush(self.queue_name, *chunk)
                pipe.publish(f"{self.queue_name}:new_data", "1")  # Уведомляем etl.push_to_elastic.genre_loader_to_elastic (появились новые данные)
                await pipe.execute()

            logger.info(f"[{self.queue_name}] Добавлено {len(chunk)} записей в очередь.")


    async def run(self) -> None:

        while True:
            
            last_modified, last_id = await self.get_last_state()

            # читаем данные из Postgres порциями по self.batch_size
            batch, last_modified, last_id = await self.fetch_main_batch(last_modified, last_id)
            
            if not batch:
                logger.debug(f"[{self.__class__.__name__}] Новых данных в Postgress нет. Ожидание...")
                await self.wait_for_timer.wait()
                
            else:          
                async for related_batch in self.fetch_related_data(batch):
                        
                    serialized_batch = [self.serialize_record(row) for row in related_batch]
                    
                    await self.push_to_queue(serialized_batch)

                await self.save_state(last_modified, last_id)
                self.wait_for_timer.reset()
