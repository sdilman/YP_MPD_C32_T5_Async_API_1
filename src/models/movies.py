from pydantic import BaseModel
from typing import List, Optional
from .base import UUIDMixin, ConfigBase


class Genre(UUIDMixin):
    name: str
    description: str

    class Config(ConfigBase):
        pass


class Person(UUIDMixin):
    name: str  # TODO: In ES - name, in the task - full_name

    class Config(ConfigBase):
        pass


class Actor(Person):
    pass


class Writer(Person):
    pass


class Director(Person):
    pass


class Film(UUIDMixin):
    title: str
    imdb_rating: float
    description: Optional[str] = None  # TODO: Should this field be nullable?

    genre: Optional[List[Genre]] = None
    actors: Optional[List[Actor]] = None
    writers: Optional[List[Writer]] = None
    directors: Optional[List[Director]] = None
    
    class Config(ConfigBase):
        pass