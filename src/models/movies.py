from pydantic import BaseModel, Field
from typing import List, Optional
from .base import UUIDMixin
from uuid import UUID


class Genre(UUIDMixin):
    name: str
    description: str


class FilmsWithPerson(BaseModel):
    id: UUID = Field(primary_key=True, validation_alias='filmwork_id')
    roles: str
    title: str
    imdb_rating: float

class Person(UUIDMixin):
    full_name: str  # TODO: In ES - name, in the task - full_name
    films: list[FilmsWithPerson]


class Actor(Person):
    pass


class Writer(Person):
    pass


class Director(Person):
    pass


class Film(UUIDMixin):
    title: str
    imdb_rating: float
    description: Optional[str]
    #genres: Optional[List[Genre]]
    genres: Optional[List[str]]
    actors: Optional[List[Actor]]
    writers: Optional[List[Writer]]
    directors: Optional[List[Director]]
