import json
import logging
from typing import List, Dict, Any

from pydantic import ValidationError

from model.models import FilmModel
from push_to_elastic.base_loader import BaseElasticConsumer
from push_to_elastic.elastic_settings.movies import MOVIES_SETTINGS

logger = logging.getLogger(__name__)

class FilmLoader(BaseElasticConsumer[FilmModel]):
    """Загрузчик фильмов в Elasticsearch."""

    def get_index_settings(self) -> dict:
        """Возвращает настройки индекса для фильмов."""
        return MOVIES_SETTINGS

    def validate_batch(self, docs_json: List[str]) -> List[FilmModel]:
        """
        Валидирует и преобразует JSON документы в модели фильмов.
        Args:
            docs_json: Список JSON строк с данными фильмов
        Returns:
            List[FilmModel]: Список валидных моделей фильмов
        """
        
        valid_films = []
        
        for film_str in docs_json:
            try:
                film_dict = json.loads(film_str)
                film = FilmModel(**film_dict)
                valid_films.append(film)
                
            except (ValidationError, json.JSONDecodeError) as e:
                logger.error(
                    f"Ошибка валидации фильма:\n"
                    f"Данные: {film_str}\n"
                    f"Ошибка: {str(e)}"
                )
                
        return valid_films


    def prepare_bulk_payload(self, valid_docs: List[FilmModel]) -> List[Dict[str, Any]]:
        """
        Подготавливает список действий для bulk-загрузки фильмов.
        Args:
            valid_docs: Список валидных моделей фильмов
        Returns:
            List[Dict[str, Any]]: Список действий для bulk API
        """
        return [
            {
                "_op_type": "index",
                "_index": self.es_index,
                "_id": doc.id,
                "_source": json.loads(doc.model_dump_json())
            }
            for doc in valid_docs
        ]