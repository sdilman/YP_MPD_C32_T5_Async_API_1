import random
import uuid
import pytest
import string
import random

from http import HTTPStatus

from tests.functional.settings import test_settings


es_index = 'genres'

@pytest.mark.asyncio
async def test_list(es_write_data, get_list_data_from_api):
    data_len = random.randint(1, 100)
    es_data = [{
        'id': str(uuid.uuid4()),
        'name': 'some_name',
    } for _ in range(data_len)]
    await es_write_data(es_data, es_index)
    url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}/api/v1/genres/?size={data_len}'
    body, headers, status = await get_list_data_from_api(url)

    assert status == HTTPStatus.OK
    assert len(body["items"]) == data_len

@pytest.mark.asyncio
async def test_get_by_id(es_write_data, get_data_from_api):
    genre_id = str(uuid.uuid4())
    name = random.choice(string.ascii_letters)
    es_data = [{
        'id': genre_id,
        'name': name,
    }]

    await es_write_data(es_data, es_index)
    url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}/api/v1/genres/{genre_id}'
    body, headers, status = await get_data_from_api(url)

    assert status == HTTPStatus.OK
    assert body["id"] == genre_id

@pytest.mark.asyncio
async def test_get_by_id_not_found(es_write_data, get_data_from_api):
    genre_id = str(uuid.uuid4())
    name = random.choice(string.ascii_letters)
    es_data = [{
        'id': str(uuid.uuid4()),
        'name': name,
    }]

    await es_write_data(es_data, es_index)
    url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}/api/v1/genres/{genre_id}'
    body, headers, status = await get_data_from_api(url)

    assert status == HTTPStatus.NOT_FOUND
    assert body["detail"] == 'genre not found'

@pytest.mark.asyncio
async def test_get_by_id_from_cache(redis_client, es_write_data, get_data_from_api, es_delete_data):
    genre_id = str(uuid.uuid4())
    name = random.choice(string.ascii_letters)
    es_data = [{
        'id': genre_id,
        'name': name,
    }]

    await es_write_data(es_data, es_index)
    url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}/api/v1/genres/{genre_id}'
    await get_data_from_api(url)

    await es_delete_data(es_index)
    body, headers, status = await get_data_from_api(url)

    assert status == HTTPStatus.OK
    assert body["id"] == genre_id
    assert body["name"] == name