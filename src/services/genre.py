from functools import lru_cache
from typing import Optional

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.movies import Genre

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class GenreService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    # get_by_id возвращает объект фильма. Он опционален, так как фильм может отсутствовать в базе
    async def get_by_id(self, genre_id: str) -> Genre | None:
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

        genre = await self._get_genre_from_elastic(genre_id)
        return genre

    async def _get_genre_from_elastic(self, genre_id: str) -> Genre | None:
        try:
            doc = await self.elastic.get(index='genres', id=genre_id)
        except NotFoundError:
            return None
        return Genre(**doc['_source'])

    async def _genre_from_cache(self, genre_id: str) -> Genre | None:
        # Пытаемся получить данные о фильме из кеша, используя команду get
        # https://redis.io/commands/get/
        data = await self.redis.get(genre_id)
        if not data:
            return None

        # pydantic предоставляет удобное API для создания объекта моделей из json
        genre = Genre.parse_raw(data)
        return genre

    async def _put_genre_to_cache(self, genre: Genre):
        # Сохраняем данные о фильме, используя команду set
        # Выставляем время жизни кеша — 5 минут
        # https://redis.io/commands/set/
        # pydantic позволяет сериализовать модель в json
        await self.redis.set(genre.id, genre.json(), FILM_CACHE_EXPIRE_IN_SECONDS)


    async def get_all(self) -> list[Genre] | None:
        try:
            docs = await self.elastic.search(index='genres',
                                             size='1000',
                                             filter_path='hits.hits._source',
                                             query={'match_all': {}}
                                             )
            if not docs:
                return None
            all_docs = [doc["_source"] for doc in docs["hits"]["hits"]]
        except NotFoundError:
            return None
        return all_docs


@lru_cache()
def get_genre_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> GenreService:
    return GenreService(redis, elastic)
