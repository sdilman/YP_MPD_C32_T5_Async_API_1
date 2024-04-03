import uuid
import pytest
import time

from http import HTTPStatus

from tests.functional.settings import test_settings


es_index = 'movies'


@pytest.fixture
def test_films_search_data():
    data = [
        {'id': str(uuid.uuid4()), 'title': 'wow movie', 'description': '', 'imdb_rating': 9.0, 'genres': []},
        {'id': str(uuid.uuid4()), 'title': 'so-so movie', 'description': '', 'imdb_rating': 6.5, 'genres': []},
    ]
    return data

@pytest.mark.asyncio
async def test_films_search_query_present(http_session, es_write_data, get_list_data_from_api, test_films_search_data):
    await es_write_data(test_films_search_data, es_index)
    query = "wow"
    time.sleep(3)
    url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}' \
          f'/api/v1/films/?query={query}&page=1&size=50'
    res, headers, status = await get_list_data_from_api(url)

    assert status == HTTPStatus.OK
    assert len(res) == 1

@pytest.mark.asyncio
async def test_films_search_query_missing(http_session, es_write_data, get_list_data_from_api, test_films_search_data):
    await es_write_data(test_films_search_data, es_index)
    query = "great"
    time.sleep(3)
    url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}' \
          f'/api/v1/films/?query={query}&page=1&size=50'
    res, headers, status = await get_list_data_from_api(url)

    assert status == HTTPStatus.NOT_FOUND
    assert res == { "detail": "films not found" }