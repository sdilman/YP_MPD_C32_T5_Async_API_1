from functools import lru_cache
from typing import Optional, List

from elasticsearch import AsyncElasticsearch, NotFoundError
from fastapi import Depends
from redis.asyncio import Redis

from db.elastic import get_elastic
from db.redis import get_redis
from models.movies import Film

FILM_CACHE_EXPIRE_IN_SECONDS = 60 * 5  # 5 минут


class FilmService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    # get_by_id возвращает объект фильма. Он опционален, так как фильм может отсутствовать в базе
    async def get_by_id(self, film_id: str) -> Optional[Film]:
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
        #
        # return film

        film = await self._get_film_from_elastic(film_id)
        return film

    async def _get_film_from_elastic(self, film_id: str) -> Optional[Film]:
        try:
            doc = await self.elastic.get(index='movies', id=film_id)
        except NotFoundError:
            return None
        return Film(**doc['_source'])

    async def get(self, genre: str = None):
        films = await self._get_films_from_elastic(genre)
        return films

    async def _get_films_from_elastic(self, genre: str = None):
        try:
            if not genre:
                docs = await self.elastic.search(index='movies')
            else:
                query = {
                    "query": {
                        "match": {
                            "genres": genre
                        }
                    }
                }
                docs = await self.elastic.search(index='movies', body=query)
        except NotFoundError:
            return None
        return [Film(**doc['_source']) for doc in docs['hits']['hits']]

    async def _film_from_cache(self, film_id: str) -> Optional[Film]:
        # Пытаемся получить данные о фильме из кеша, используя команду get
        # https://redis.io/commands/get/
        data = await self.redis.get(film_id)
        if not data:
            return None

        # pydantic предоставляет удобное API для создания объекта моделей из json
        film = Film.parse_raw(data)
        return film

    async def _put_film_to_cache(self, film: Film):
        # Сохраняем данные о фильме, используя команду set
        # Выставляем время жизни кеша — 5 минут
        # https://redis.io/commands/set/
        # pydantic позволяет сериализовать модель в json
        await self.redis.set(film.id, film.json(), FILM_CACHE_EXPIRE_IN_SECONDS)


@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)
