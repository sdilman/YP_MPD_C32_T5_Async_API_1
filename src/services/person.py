import pickle
from functools import lru_cache

from db.elastic import get_elastic
from db.redis import get_redis
from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from models.movies import FilmsWithPerson, Person
from redis.asyncio import Redis

PERSON_CACHE_EXPIRE_IN_SECONDS = 60 * 5


class PersonService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, person_id: str) -> Person | None:
        person = await self._person_from_cache(person_id)
        if not person:
            person = await self._get_person_from_elastic(person_id)
            if not person:
                return None
            await self._put_person_to_cache(person.uuid, person)
        return person

    async def _get_person_from_elastic(self, person_id: str) -> Person | None:
        try:
            doc = await self.elastic.get(index='persons', id=person_id)
        except NotFoundError:
            return None
        return Person(**doc['_source'])

    async def _person_from_cache(self, key: str) -> Person | None:
        data = await self.redis.get(key)
        if not data:
            return None
        return pickle.loads(data)

    async def _put_person_to_cache(self, key, person: Person):
        await self.redis.set(str(key), pickle.dumps(person), PERSON_CACHE_EXPIRE_IN_SECONDS)

    async def _search_person_from_elastic(self, phrase: str) -> list[Person] | None:
        try:
            docs = await self.elastic.search(
                index='persons',
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
            return person.films
        return None

    async def get_by_search(self, phrase: str) -> list[Person] | None:
        persons = await self._person_from_cache(phrase)
        if not persons:
            persons = await self._search_person_from_elastic(phrase)
            if not persons:
                return None
            await self._put_person_to_cache(phrase, persons)

        return persons


@lru_cache()
def get_person_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    return PersonService(redis, elastic)
