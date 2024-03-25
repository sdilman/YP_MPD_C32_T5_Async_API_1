from functools import lru_cache
from typing import Optional

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.movies import Person, FilmsWithPerson

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class PersonService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    # get_by_id возвращает объект фильма. Он опционален, так как фильм может отсутствовать в базе
    async def get_by_id(self, person_id: str) -> Person | None:
        # # Пытаемся получить данные из кеша, потому что оно работает быстрее
        # film = await self._film_from_cache(film_id)
        # if not film:
        #     # Если фильма нет в кеше, то ищем его в Elasticsearch
        #     film = await self._get_film_from_elastic(film_id)
        #     if not film:
        #         # Если он отсутствует в Elasticsearch, значит, фильма вообще нет в базе
        #         return None
        #     # Сохраняем фильм  в кеш
        #     await self._put_film_to_cache(film)

        # return film

        person = await self._get_person_from_elastic(person_id)
        return person

    async def _get_person_from_elastic(self, person_id: str) -> Person | None:
        try:
            doc = await self.elastic.get(index='persons', id=person_id)
            print(doc)
        except NotFoundError:
            return None
        return Person(**doc['_source'])

    async def _person_from_cache(self, person_id: str) -> Person | None:
        # Пытаемся получить данные о фильме из кеша, используя команду get
        # https://redis.io/commands/get/
        data = await self.redis.get(person_id)
        if not data:
            return None

        # pydantic предоставляет удобное API для создания объекта моделей из json
        person = Person.parse_raw(data)
        return person

    async def _put_person_to_cache(self, person: Person):
        # Сохраняем данные о фильме, используя команду set
        # Выставляем время жизни кеша — 5 минут
        # https://redis.io/commands/set/
        # pydantic позволяет сериализовать модель в json
        await self.redis.set(person.id, person.json(), FILM_CACHE_EXPIRE_IN_SECONDS)


    async def _search_person_from_elastic(self, phrase: str) -> list[Person] | None:
        try:
            docs = await self.elastic.search(index='persons',
                                            filter_path='hits.hits._source',
                                            query={
                                                    "match": {
                                                        "full_name": {
                                                            "query": phrase,
                                                            "fuzziness": "auto"
                                                          }
                                                        }
                                                    }
                                            )
            if not docs:
                return None
            all_docs = [doc["_source"] for doc in docs["hits"]["hits"]]
        except NotFoundError:
            return None
        return all_docs

    async def films_with_person(self, person_id: str) -> list[FilmsWithPerson] | None:
        person = await self.get_by_id(person_id)
        if person:
            print(person)
            return person.films
        return None

    async def get_by_search(self, phrase: str) -> list[Person] | None:
        persons = await self._search_person_from_elastic(phrase)
        return persons


@lru_cache()
def get_person_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis, elastic)
