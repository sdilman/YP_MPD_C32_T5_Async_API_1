import uuid
import pytest
import time
import json

from http import HTTPStatus

from tests.functional.settings import test_settings


es_index = 'movies'


@pytest.fixture
def test_films_search_data():
    data = [
        {'id': str(uuid.uuid4()), 'title': 'wow movie', 'description': '', 
         'imdb_rating': 9.0, 'genres': ['Sci-Fi', 'Adventure']},
        {'id': str(uuid.uuid4()), 'title': 'so-so movie', 'description': '', 
         'imdb_rating': 6.5, 'genres': []},
        {'id': str(uuid.uuid4()), 'title': 'doc film', 'description': '',
          'imdb_rating': 6.5, 'genres': ['Documentary']},
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

@pytest.mark.asyncio
async def test_films_search_query_sort(http_session, es_write_data, get_list_data_from_api, test_films_search_data):
    await es_write_data(test_films_search_data, es_index)
    sort_term = "-imdb_rating"
    time.sleep(3)
    url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}' \
          f'/api/v1/films/?sort={sort_term}&page=1&size=50'
    res, headers, status = await get_list_data_from_api(url)
    data_sorted = sorted(test_films_search_data, key=lambda x: x['imdb_rating'], reverse=True)

    assert status == HTTPStatus.OK
    assert len(res) == len(data_sorted)
    res[0]['imdb_rating'] == data_sorted[0]['imdb_rating']
    res[1]['imdb_rating'] == data_sorted[1]['imdb_rating']

@pytest.mark.asyncio
async def test_films_search_query_genre(http_session, es_write_data, get_list_data_from_api, test_films_search_data):
    await es_write_data(test_films_search_data, es_index)
    genre = "Documentary"
    time.sleep(3)
    url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}' \
          f'/api/v1/films/?genre={genre}&page=1&size=50'
    res, headers, status = await get_list_data_from_api(url)

    assert status == HTTPStatus.OK
    assert len(res) == 1

@pytest.mark.asyncio
async def test_films_search_id(http_session, es_write_data, get_data_from_api, test_films_search_data):
    await es_write_data(test_films_search_data, es_index)
    film_id = test_films_search_data[0]['id']
    time.sleep(3)
    url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}' \
          f'/api/v1/films/{film_id}'
    body, headers, status = await get_data_from_api(url)

    assert status == HTTPStatus.OK
    assert body['id'] == film_id

@pytest.mark.asyncio
async def test_films_search_title(http_session, es_write_data, get_list_data_from_api, test_films_search_data):
    await es_write_data(test_films_search_data, es_index)
    phrase = 'movie'
    time.sleep(3)
    url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}' \
          f'/api/v1/films/search?phrase={phrase}&page=1&size=50'
    res, headers, status = await get_list_data_from_api(url)

    assert status == HTTPStatus.OK
    assert len(res) == 2