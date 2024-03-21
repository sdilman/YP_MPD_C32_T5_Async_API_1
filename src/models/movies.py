from pydantic import BaseModel
from typing import List, Optional
from .base import UUIDMixin


class Genre(UUIDMixin):
    name: str
    description: str


class Person(UUIDMixin):
    name: str  # TODO: In ES - name, in the task - full_name


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
