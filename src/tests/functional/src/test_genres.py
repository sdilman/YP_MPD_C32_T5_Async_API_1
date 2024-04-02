import uuid
import pytest
import time

from http import HTTPStatus

from tests.functional.settings import test_settings


es_index = 'genres'

@pytest.mark.asyncio
async def test_list(http_session, es_write_data, get_data_from_api):
    # 1. Генерируем данные для ES
    es_data = [{
        'id': str(uuid.uuid4()),
        'name': 'some_name',
    } for _ in range(50)]

    await es_write_data(es_data, es_index)

    # 3. Запрашиваем данные из ES по API
    time.sleep(3)
    url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}/api/v1/genres'
    body, headers, status = await get_data_from_api(url)

    # 4. Проверяем ответ
    assert status == HTTPStatus.OK
    assert len(body["items"]) == 50


# @pytest.mark.asyncio
# async def test_get_by_id(http_session, es_write_data, get_data_from_api):
#     # 1. Генерируем данные для ES
#     genre_id = str(uuid.uuid4())
#     es_data = [{
#         'id': genre_id,
#         'name': 'some_name',
#     }]
#
#     await es_write_data(es_data, es_index)
#
#     # 3. Запрашиваем данные из ES по API
#     time.sleep(3)
#
#     url = f'http://{test_settings.FASTAPI_HOST}:{test_settings.FASTAPI_PORT}/api/v1/genres/{genre_id}'
#     body, headers, status = await get_data_from_api(url)
#
#     # 4. Проверяем ответ
#     assert status == HTTPStatus.OK
#     assert body["id"] == genre_id