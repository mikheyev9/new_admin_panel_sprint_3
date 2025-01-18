import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any, List

import aiohttp
from pydantic import ValidationError

from transform.models import FilmModel
from load.movies_index import MOVIES_SETTINGS
from utils.waiting import WaitingManager
from utils.backoff import backoff

logger = logging.getLogger(__name__)


@dataclass
class FilmConsumer:
    redis_conn: Any
    es_base_url: str
    batch_size: int = 10
    queue_name: str = "film_queue"
    es_index: str = "movies"


    def __post_init__(self):
        self.es_bulk_url = f"{self.es_base_url}/_bulk"
        
        
    async def __index_exists(self) -> bool:
        """Проверяет, существует ли индекс в Elasticsearch."""
        
        async with aiohttp.ClientSession() as session:
            async with session.head(f"{self.es_base_url}/{self.es_index}") as resp:
                if resp.status == 200:
                    logger.info(f"Индекс {self.es_index} уже существует.")
                    return True
                logger.info(f"Индекс {self.es_index} не найден.")
                return False


    async def __create_index(self) -> None:
        """Создаёт индекс в Elasticsearch с настройками и маппингами."""
        
        async with aiohttp.ClientSession() as session:
            async with session.put(f"{self.es_base_url}/{self.es_index}", json=MOVIES_SETTINGS) as resp:
                if resp.status == 200:
                    logger.info(f"Индекс {self.es_index} успешно создан.")
                else:
                    logger.error(f"Ошибка создания индекса {self.es_index}: {await resp.text()}")


    @backoff(exceptions=(aiohttp.ClientError, asyncio.TimeoutError))
    async def ensure_index_exists(self) -> None:
        """Проверяет наличие индекса и создаёт его при отсутствии."""
        if not await self.__index_exists():
            await self.__create_index()


    @backoff(exceptions=(ConnectionError, asyncio.TimeoutError))
    async def fetch_from_queue(self) -> List[str]:
        
        queue_length = await self.redis_conn.llen(self.queue_name)

        if queue_length == 0:
            return []

        fetch_count = min(queue_length, self.batch_size)
        data = await self.redis_conn.lrange(self.queue_name, 0, fetch_count - 1)

        await self.redis_conn.ltrim(self.queue_name, fetch_count, -1)

        logger.debug(f"Извлечено {len(data)} элементов из очереди.")
        return data


    @staticmethod
    def validate_films(films_json: List[str]) -> List[FilmModel]:
        
        valid_films = []
        
        for film_str in films_json:
            
            try:
                film_dict = json.loads(film_str)
                film = FilmModel(**film_dict)
                valid_films.append(film)
                
            except (ValidationError, json.JSONDecodeError) as e:
                logger.error(f"Ошибка валидации фильма: {e}\nДанные: {film_str}")
                
        return valid_films

    @staticmethod
    def prepare_bulk_payload(films: List[FilmModel]) -> str:
        
        bulk_lines = []

        for film in films:
            
            index_line = json.dumps({"index": {"_index": "movies", "_id": film.id}})
            film_line = film.model_dump_json()
            
            bulk_lines.append(index_line)
            bulk_lines.append(film_line)

        # Объединяем строки в NDJSON-формат
        payload = "\n".join(bulk_lines) + "\n"
        
        return payload


    @backoff(exceptions=(aiohttp.ClientError, asyncio.TimeoutError))
    async def send_bulk_to_es(self, payload: str) -> None:
        
        async with aiohttp.ClientSession() as session:
            
            headers = {"Content-Type": "application/x-ndjson"}
            
            async with session.post(self.es_bulk_url, headers=headers, data=payload) as resp:
                
                response = await resp.json()
                
                if resp.status != 200:
                    logger.error(f"Ошибка bulk индексации: {resp.status}\nОтвет: {response}")
                    
                else:
                    logger.info(f"Успешно отправлено {len(payload.splitlines()) // 2} фильмов в ES. "
                                f"Ответ: {str(response)[:400]}")


    async def run(self):
        wait = WaitingManager(start_time=1.0, max_time=60.0, factor=2.0)
        
        await self.ensure_index_exists()
        
        while True:
            
            films_dicts = await self.fetch_from_queue()
            #print('films_dicts', films_dicts)
            
            if not films_dicts:
                
                logger.debug("Очередь Redis пуста. Ожидание следующей попытки...")
                await wait.wait()
                
            else:
                
                valid_films = self.validate_films(films_dicts)
                data_to_send = self.prepare_bulk_payload(valid_films)
                
                if data_to_send:
                    
                    await self.send_bulk_to_es(data_to_send)
                    
                    logger.info(f'Sent {len(valid_films)} valid films to Elasticsearch')
                    
                wait.reset()

