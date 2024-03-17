import uuid
from dataclasses import dataclass, field


@dataclass
class FilmWork:
    id: uuid.UUID
    title: str
    description: str
    imdb_rating: float
    persons: list[dict]
    genre: list[str]
    actors_names: list = field(init=False)
    writers_names: list = field(init=False)
    directors_names: list = field(init=False)
    actors: list = field(init=False)
    writers: list = field(init=False)
    directors: list = field(init=False)

    def __post_init__(self):
        self.actors_names, self.actors = [], []
        self.writers_names, self.writers = [], []
        self.directors_names, self.directors = [], []
        for person in self.persons:
            if person["person_role"] == "actor":
                self.actors_names.append(person["person_name"])
                person_dict = {"id": person["person_id"], "name": person["person_name"]}
                self.actors.append(person_dict)
            elif person["person_role"] == "writer":
                self.writers_names.append(person["person_name"])
                person_dict = {"id": person["person_id"], "name": person["person_name"]}
                self.writers.append(person_dict)
            elif person["person_role"] == "director":
                self.directors_names.append(person["person_name"])
                person_dict = {"id": person["person_id"], "name": person["person_name"]}
                self.directors.append(person_dict)
