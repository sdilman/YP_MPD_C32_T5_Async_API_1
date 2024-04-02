import pytest
import aiohttp
import asyncio


from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from tests.functional.settings import test_settings
from tests.functional.testdata.es_mapping import schema_genres


@pytest.fixture(scope='session')
async def es_client():
    client = AsyncElasticsearch(hosts=f'http://{test_settings.ELASTIC_HOST}:{test_settings.ELASTIC_PORT}')
    for es_index in ['films', 'genres', 'persons']:
        if await client.indices.exists(index=es_index):
            await client.indices.delete(index=es_index)
        await client.indices.create(index=es_index, **schema_genres)
    yield client
    await client.close()

@pytest.fixture(scope='session')
async def http_session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()

def get_es_bulk_query(es_data, es_index):
    bulk_query: list[dict] = []
    for row in es_data:
        data = {'_index': es_index, '_id': row['id']}
        data.update({'_source': row})
        bulk_query.append(data)
    return bulk_query

@pytest.fixture
def es_write_data(es_client):
    async def inner(data: list[dict], es_index: str):
        bulk_query = get_es_bulk_query(data, es_index)
        updated, errors = await async_bulk(client=es_client, actions=bulk_query)
        if errors:
            raise Exception('Ошибка записи данных в Elasticsearch')
    return inner

@pytest.fixture
def get_data_from_api(http_session):
    async def inner(url: str):
        async with http_session.get(url) as response:
            body = await response.json()
            headers = response.headers
            status = response.status
        return body, headers, status
    return inner


# @pytest.fixture(scope="session")
# def event_loop():
#     try:
#         loop = asyncio.get_running_loop()
#     except RuntimeError:
#         loop = asyncio.new_event_loop()
#     yield loop
#     loop.close()