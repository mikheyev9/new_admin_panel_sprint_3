import json
import logging
from typing import List

from pydantic import ValidationError
from model.models import PersonElasticModel
from push_to_elastic.base_loader import BaseElasticConsumer
from push_to_elastic.elastic_settings.persons import PERSONS_SETTINGS

logger = logging.getLogger(__name__)

class PersonLoader(BaseElasticConsumer[PersonElasticModel]):
    """Загрузчик персон в Elasticsearch."""

    def get_index_settings(self) -> dict:
        """Возвращает настройки индекса для персон."""
        return PERSONS_SETTINGS

    def validate_batch(self, docs_json: List[str]) -> List[PersonElasticModel]:
        """Валидирует данные для загрузки в Elasticsearch."""
        valid_persons = []

        for person_str in docs_json:
            try:
                person_dict = json.loads(person_str)
                person = PersonElasticModel(**person_dict)
                valid_persons.append(person)
            except (ValidationError, json.JSONDecodeError) as e:
                logger.error(
                    f"Ошибка валидации персоны:\n"
                    f"Данные: {person_str}\n"
                    f"Ошибка: {str(e)}"
                )
        
        return valid_persons

    def prepare_bulk_payload(self, valid_docs: List[PersonElasticModel]) -> List[dict]:
        """Подготавливает данные для массовой загрузки в Elasticsearch."""
        return [
            {
                "_op_type": "index",
                "_index": self.es_index,
                "_id": person.id,
                "_source": json.loads(person.model_dump_json())
            }
            for person in valid_docs
        ]
