import json
from pydantic import BaseModel, Field, field_validator, model_validator, ValidationError
from typing import List, Optional, Union
from uuid import UUID
from datetime import datetime


class Person(BaseModel):
    id: str = Field(default="")
    name: str


class FilmModel(BaseModel):
    id: str
    modified: Optional[str] = None
    title: str
    description: str = Field(default="")
    imdb_rating: Optional[float] = None
    genres: List[str] = Field(default_factory=list)

    actors_names: List[str] = Field(default_factory=list)
    directors_names: List[str] = Field(default_factory=list)
    writers_names: List[str] = Field(default_factory=list)

    actors: List[Person] = Field(default_factory=list)
    directors: List[Person] = Field(default_factory=list)
    writers: List[Person] = Field(default_factory=list)


    @field_validator("id", mode="before")
    def validate_id(cls, value: Union[UUID, str]) -> str:
        if isinstance(value, UUID):
            return str(value)
        if isinstance(value, str):
            try:
                UUID(value)
                return value
            except ValueError:
                raise ValueError(f"Некорректное значение для поля `id`: {value}")
        raise ValueError(f"Поле `id` должно быть UUID или строкой, а не {type(value)}")


    @field_validator("modified", mode="before")
    def ensure_modified_is_isoformat(cls, value: Union[datetime, str]) -> str:
        if isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, str):
            try:
                datetime.fromisoformat(value)
                return value
            except ValueError:
                raise ValueError(f"Некорректное значение для поля `modified`: {value}")
        raise ValueError(f"Поле `modified` должно быть строкой или datetime, а не {type(value)}")


    @model_validator(mode="before")
    def ensure_lists_and_convert(cls, values: dict) -> dict:
        """
        Подготовка данных:
        1. Заменяет None на пустые списки.
        2. Если поля actors, directors, writers — это JSON-строки, парсит их.
        3. Заполняет поля *_names из nested объектов.
        """
        list_fields = ["genres", "actors_names", "directors_names", "writers_names", "actors", "directors", "writers"]
        for field_name in list_fields:
            if values.get(field_name) is None:
                values[field_name] = []

        # Remove None values from list fields
        for field_name in list_fields:
            if isinstance(values[field_name], list):
                values[field_name] = [item for item in values[field_name] if item is not None]

        for nested_field in ["actors", "directors", "writers"]:
            val = values.get(nested_field)
            if isinstance(val, str):
                try:
                    parsed_val = json.loads(val)
                    values[nested_field] = parsed_val
                except json.JSONDecodeError:
                    values[nested_field] = []

        values["actors_names"] = [actor["name"] for actor in values["actors"]]
        values["directors_names"] = [director["name"] for director in values["directors"]]
        values["writers_names"] = [writer["name"] for writer in values["writers"]]

        return values


    @field_validator("description", mode="before")
    def handle_null_description(cls, value: Optional[str]) -> str:
        return value or ""


    def model_dump_json(self) -> str:
        """
        Возвращает сериализованную модель в формате JSON для Elasticsearch.
        """
        data_dict = self.model_dump(exclude_none=True)
        data_dict.pop("modified", None)
        return json.dumps(data_dict, ensure_ascii=False)

