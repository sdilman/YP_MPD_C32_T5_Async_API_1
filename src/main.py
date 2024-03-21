import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from redis.cluster import RedisCluster

from api.v1 import films
from core import config
from core.logger import LOGGING
from db import elastic, redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    # redis.rc = RedisCluster(host=config.REDIS_HOST, port=config.REDIS_PORT)
    # print(redis.rc.get_nodes())
    await elastic.es.info()
    yield
    # redis.rc.close()
    elastic.es.close()


app = FastAPI(
    title=config.PROJECT_NAME,
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    default_response_class=ORJSONResponse,
    lifespan=lifespan
)


app.include_router(films.router, prefix='/api/v1/films', tags=['films'])


if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host='0.0.0.0',
        port=8000,
        log_config=LOGGING,
        log_level=logging.DEBUG,
    )
