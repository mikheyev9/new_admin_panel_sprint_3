import json
import logging
from typing import List, Dict, Any

from pydantic import ValidationError

from model.models import GenreModel
from push_to_elastic.base_loader import BaseElasticConsumer
from push_to_elastic.elastic_settings.genres import GENRE_SETTINGS

logger = logging.getLogger(__name__)


class GenreLoader(BaseElasticConsumer[GenreModel]):
    """Загрузчик жанров в Elasticsearch."""

    def get_index_settings(self) -> dict:
        return GENRE_SETTINGS


    def validate_batch(self, docs_json: List[str]) -> List[GenreModel]:
        """
        Валидирует и преобразует JSON-документы в модели жанров.
        Args:
            docs_json: Список JSON строк с данными жанров
        Returns:
            List[GenreModel]: Список валидных моделей жанров
        """
        
        valid_genres = []
        
        for genre_str in docs_json:
            try:
                genre_dict = json.loads(genre_str)
                genre = GenreModel(**genre_dict)
                valid_genres.append(genre)

            except (ValidationError, json.JSONDecodeError) as e:
                logger.error(
                    f"Ошибка валидации жанра:\n"
                    f"Данные: {genre_str}\n"
                    f"Ошибка: {str(e)}"
                )

        return valid_genres

    def prepare_bulk_payload(self, valid_docs: List[GenreModel]) -> List[Dict[str, Any]]:
        """
        Подготавливает список действий для bulk-загрузки жанров.
        Args:
            valid_docs: Список валидных моделей жанров
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
