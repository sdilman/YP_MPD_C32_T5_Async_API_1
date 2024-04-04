import random
import uuid
import pytest
import string
import random

from http import HTTPStatus

from tests.functional.settings import test_settings


es_index = 'persons'

@pytest.mark.asyncio
async def test_get_by_id(http_session, es_write_data, get_data_from_api):
    person_id = str(uuid.uuid4())
    film_id = str(uuid.uuid4())
    full_name = random.choice(string.ascii_letters)
    roles = random.choice(string.ascii_letters)
    es_data = [{
        'id': person_id,
        'full_name': full_name,
        'roles': roles,
        'films': [{
            "filmwork_id": film_id,
            "roles": [
                'some_role'
            ],
            'title': 'some_title',
            'imdb_rating': 4.7
        }]
    }]
    await es_write_data(es_data, es_index)
    url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}/api/v1/persons/{person_id}'
    body, headers, status = await get_data_from_api(url)

    assert status == HTTPStatus.OK
    assert body["uuid"] == person_id

@pytest.mark.asyncio
async def test_get_by_id_not_found(http_session, es_write_data, get_data_from_api):
    person_id = str(uuid.uuid4())
    film_id = str(uuid.uuid4())
    full_name = random.choice(string.ascii_letters)
    roles = random.choice(string.ascii_letters)
    es_data = [{
        'id': str(uuid.uuid4()),
        'full_name': full_name,
        'roles': roles,
        'films': [{
            "filmwork_id": film_id,
            "roles": [
                'some_role'
            ],
            'title': 'some_title',
            'imdb_rating': 4.7
        }]
    }]
    await es_write_data(es_data, es_index)
    url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}/api/v1/persons/{person_id}'
    body, headers, status = await get_data_from_api(url)

    assert status == HTTPStatus.NOT_FOUND
    assert body["detail"] == 'person not found'

@pytest.mark.asyncio
async def test_get_by_id_from_cache(http_session, es_write_data, get_data_from_api, es_delete_data):
    person_id = str(uuid.uuid4())
    film_id = str(uuid.uuid4())
    full_name = random.choice(string.ascii_letters)
    roles = random.choice(string.ascii_letters)
    es_data = [{
        'id': person_id,
        'full_name': full_name,
        'roles': roles,
        'films': [{
            "filmwork_id": film_id,
            "roles": [
                'some_role'
            ],
            'title': 'some_title',
            'imdb_rating': 4.7
        }]
    }]
    await es_write_data(es_data, es_index)
    url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}/api/v1/persons/{person_id}'
    await get_data_from_api(url)
    await es_delete_data(es_index)
    body, headers, status = await get_data_from_api(url)

    assert status == HTTPStatus.OK
    assert body["uuid"] == person_id

@pytest.mark.asyncio
async def test_search(http_session, es_write_data, get_data_from_api, es_delete_data):
    phrase = 'Krill'
    es_data = [{
        'id': str(uuid.uuid4()),
        'full_name': 'Kirill',
        'roles': 'actor',
        'films': [{
            "filmwork_id": str(uuid.uuid4()),
            "roles": [
                'some_role'
            ],
            'title': 'some_title',
            'imdb_rating': 4.7
        }]
    },
        {
            'id': str(uuid.uuid4()),
            'full_name': 'Stepan',
            'roles': 'actor',
            'films': [{
                "filmwork_id": str(uuid.uuid4()),
                "roles": [
                    'some_role'
                ],
                'title': 'some_title',
                'imdb_rating': 4.7
            }]
        }
    ]
    await es_write_data(es_data, es_index)
    url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}/api/v1/persons/search?phrase={phrase}'
    body, headers, status = await get_data_from_api(url)

    assert status == HTTPStatus.OK
    assert len(body) == 1

@pytest.mark.asyncio
async def test_films_by_person(http_session, es_write_data, get_data_from_api, es_delete_data):
    person_id = str(uuid.uuid4())
    film_id1 = str(uuid.uuid4())
    film_id2 = str(uuid.uuid4())
    full_name = random.choice(string.ascii_letters)
    roles = random.choice(string.ascii_letters)
    es_data = [{
        'id': person_id,
        'full_name': full_name,
        'roles': roles,
        'films': [{
            "filmwork_id": film_id1,
            "roles": [
                'some_role'
            ],
            'title': 'some_title',
            'imdb_rating': 4.7
        },
        {
            "filmwork_id": film_id2,
            "roles": [
                'some_role'
            ],
            'title': 'some_title',
            'imdb_rating': 4.7
        },
        ]
    }]
    await es_write_data(es_data, es_index)
    url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}/api/v1/persons/{person_id}/films'
    body, headers, status = await get_data_from_api(url)

    assert status == HTTPStatus.OK
    assert len(body["items"]) == len(es_data[0]['films'])