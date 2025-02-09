import asyncio
import json
import logging
from typing import List, TypeVar, Dict, Generic

import aiohttp
from abc import ABC, abstractmethod
from pydantic import BaseModel
from dataclasses import dataclass
from typing import Any, List
from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import ApiError
from elasticsearch.helpers import async_bulk
from redis.asyncio.client import Pipeline
from utils.waiting import WaitingManager
from utils.backoff import backoff

logger = logging.getLogger(__name__)

ModelType = TypeVar('ModelType', bound=BaseModel)


@dataclass
class BaseElasticConsumer(ABC, Generic[ModelType]):

    redis_conn: Any
    es_client: AsyncElasticsearch
    batch_size: int = 10
    queue_name: str = "default_queue"
    es_index: str = "default_index"


    def __post_init__(self):
        self.wait = WaitingManager(
            start_time=1,
            max_time=60,
            factor=2
        )
        
    @property
    def transaction(self) -> Pipeline:
        """Returns Redis pipeline for atomic operations"""
        return self.redis_conn.pipeline()
        

    async def __index_exists(self) -> bool:
        """Проверяет, существует ли индекс в Elasticsearch."""
        
        try:
            exists = await self.es_client.indices.exists(index=self.es_index)
            if exists:
                logger.info(f"Индекс {self.es_index} уже существует.")
            else:
                logger.info(f"Индекс {self.es_index} не найден.")
            return exists
        except Exception as e:
            logger.error(f"Ошибка при проверке индекса: {e}")
            return False


    async def __create_index(self) -> None:
        """Создаёт индекс в Elasticsearch с заданными настройками и маппингом."""
        
        try:
            await self.es_client.indices.create(
                index=self.es_index,
                body=self.get_index_settings()
            )
            logger.info(f"Индекс {self.es_index} успешно создан.")
        except Exception as e:
            logger.error(f"Ошибка создания индекса {self.es_index}: {e}")


    @backoff(exceptions=(aiohttp.ClientError, asyncio.TimeoutError))
    async def ensure_index_exists(self) -> None:
        """Проверяет наличие индекса и создаёт его при отсутствии."""
        
        if not await self.__index_exists():
            await self.__create_index()


    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError))
    async def fetch_from_queue(self) -> List[str]:
        """
        Извлекает из Redis данные в количестве не более batch_size.
        
        Returns:
            List[str]: Список извлеченных элементов. Пустой список, если очередь пуста
            или произошла ошибка.
        """
        
        try:
            queue_length = await self.redis_conn.llen(self.queue_name)
            logger.debug(f"Длина очереди {self.queue_name}: {queue_length}")
            
            if queue_length == 0:
                logger.debug("Очередь пуста")
                return []

            fetch_count = min(queue_length, self.batch_size)
        
            async with self.transaction as pipe:
                pipe.lrange(self.queue_name, 0, fetch_count - 1)
                pipe.ltrim(self.queue_name, fetch_count, -1)
                
                results = await pipe.execute()
                
                if not results or len(results) < 1:
                    logger.error("Не удалось получить результаты из Redis pipeline")
                    return []
                    
                fetched_data = results[0]
                logger.debug(
                    f"Успешно извлечено {len(fetched_data)} элементов "
                    f"из очереди {self.queue_name}"
                )
                return fetched_data
                
        except Exception as e:
            logger.exception(
                f"Ошибка при извлечении данных из очереди {self.queue_name}: {str(e)}"
            )
            return []
            

    @abstractmethod
    def get_index_settings(self) -> dict:
        """
        Должен вернуть настройки (settings и mappings) для создания индекса.
        Например, для фильмов это может быть MOVIES_SETTINGS.
        """
        pass


    @abstractmethod
    def validate_batch(self, docs_json: List[str]) -> List[ModelType]:
        """
        Валидирует и преобразует документы из очереди.
        Args:
            docs_json: Список JSON строк с документами
        Returns:
            List[ModelType]: Список валидных Pydantic моделей
        """
        pass


    @abstractmethod
    def prepare_bulk_payload(self, valid_docs: List[ModelType]) -> List[Dict[str, Any]]:
        """
        Подготавливает список действий для bulk-загрузки в Elasticsearch.
        
        Args:
            valid_docs: Список валидных Pydantic моделей

        Returns:
            List[Dict[str, Any]]: Список действий для bulk API в формате:
                [
                    {
                        "index": {"_index": "имя_индекса", "_id": "идентификатор"},
                        "_source": { документ }
                    },
                    ...
                ]
        """
        pass

    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError, ApiError))
    async def send_bulk_to_es(self, actions: List[Dict[str, Any]]) -> None:
        """
        Отправляет подготовленные действия в Elasticsearch.
        Args:
            actions: Список действий для bulk API
        """
        
        try:        
            success, failed_items = await async_bulk(
                client=self.es_client,
                actions=actions,
                raise_on_error=False,
                chunk_size=self.batch_size,
                max_chunk_bytes=10 * 1024 * 1024
            )
            
            if failed_items:
                error_details = {}
                
                for item in failed_items:
                    doc = item['index']
                    doc_id = doc['_id']
                    error_info = {
                        'error_type': doc.get('error', {}).get('type'),
                        'reason': doc.get('error', {}).get('reason'),
                        'status': doc.get('status')
                    }
                    error_details[doc_id] = error_info

                logger.error(
                    f"Ошибки при bulk индексации:\n"
                    f"Всего документов: {len(actions)}\n"
                    f"Успешно: {success}\n"
                    f"Неудачно: {len(failed_items)}\n"
                    f"Детали ошибок: {json.dumps(error_details, indent=2)}"
                )
            
            else:
                logger.info(
                    f"Bulk-загрузка успешно завершена:\n"
                    f"Индекс: {self.es_index}\n"
                    f"Загружено документов: {success}"
                )
                    
        except Exception as e:
            logger.exception(
                f"Критическая ошибка при bulk индексации:\n"
                f"Индекс: {self.es_index}\n"
                f"Количество документов: {len(actions)}\n"
                f"Ошибка: {str(e)}"
            )
            raise


    async def cleanup(self):
        """Закрывает соединение с Elasticsearch."""
        await self.es_client.close()
    
    
    async def run(self):
        """
        Основной цикл: проверка наличия индекса, извлечение данных из очереди,
        валидация, формирование bulk-пейлоада и отправка в Elasticsearch.
        """
        try:
            
            await self.ensure_index_exists()

            while True:
                
                docs = await self.fetch_from_queue()
                
                if not docs:
                    logger.debug("Очередь Redis пуста. Ожидание следующей попытки...")
                    await self.wait.wait()
                    
                else:
                    
                    valid_docs: List[ModelType] = self.validate_batch(docs)
                    
                    payload: List[Dict[str, Any]] = self.prepare_bulk_payload(valid_docs)
                    
                    if payload:
                        
                        await self.send_bulk_to_es(payload)
                        
                    self.wait.reset()
                    
        finally:
            
            await self.cleanup()
