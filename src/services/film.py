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

    async def get_by_id(self, film_id: str) -> Optional[Film]:
        film = await self._film_from_cache(film_id)
        if not film:
            film = await self._get_film_from_elastic(film_id)
            if not film:
                return None
            await self._put_film_to_cache(film)
        return film

    async def _get_film_from_elastic(self, film_id: str) -> Optional[Film]:
        try:
            doc = await self.elastic.get(index='movies', id=film_id)
        except NotFoundError:
            return None
        
        _doc = doc['_source'].copy()
        _directors = [{'uuid': d['id'], 'full_name': d['name'], 'films': []} for d in _doc['directors']]
        _doc['directors'] = _directors
        _writers = [{'uuid': w['id'], 'full_name': w['name'], 'films': []} for w in _doc['writers']]
        _doc['writers'] = _writers
        _actors = [{'uuid': a['id'], 'full_name': a['name'], 'films': []} for a in _doc['actors']]
        _doc['actors'] = _actors
        return Film(**_doc)

    async def get(self, genre: str = None, title: str = None):
        films = await self._get_films_from_elastic(genre, title)
        return films

    async def _get_films_from_elastic(self, genre: str = None, title: str = None):
        try:
            if genre or title:
                cond = []
                if genre:
                    cond.append(
                        { "match": { "genres": genre } }
                    )
                if title:
                    cond.append(
                        { "match": { "title": title } }
                    )
                query = {
                    "query": {
                        "bool": {
                            "must": cond
                            }
                        }
                    }
                docs = await self.elastic.search(index='movies', body=query)
            else:
                docs = await self.elastic.search(index='movies')  
        except NotFoundError:
            return None

        res: List[Film] = []
        for doc in docs['hits']['hits']:
            _doc = doc['_source'].copy()
            _directors = [{'uuid': d['id'], 'full_name': d['name'], 'films': []} for d in _doc['directors']]
            _doc['directors'] = _directors
            _writers = [{'uuid': w['id'], 'full_name': w['name'], 'films': []} for w in _doc['writers']]
            _doc['writers'] = _writers
            _actors = [{'uuid': a['id'], 'full_name': a['name'], 'films': []} for a in _doc['actors']]
            _doc['actors'] = _actors
            res.append(Film(**_doc))
        return res

    async def _film_from_cache(self, film_id: str) -> Optional[Film]:
        data = await self.redis.get(film_id)
        if not data:
            return None
        film = Film.parse_raw(data)
        return film

    async def _put_film_to_cache(self, film: Film):
        await self.redis.set(str(film.uuid), film.json(), FILM_CACHE_EXPIRE_IN_SECONDS)

@lru_cache()
def get_film_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> FilmService:
    return FilmService(redis, elastic)
