from elasticsearch import AsyncElasticsearch

from core import config

es = AsyncElasticsearch(hosts=[f'http://{config.ELASTIC_HOST}:{config.ELASTIC_PORT}'])


async def get_elastic() -> AsyncElasticsearch:
    return es
