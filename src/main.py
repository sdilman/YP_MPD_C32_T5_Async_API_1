import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from api.v1 import films, persons, genres
from core import config
from core.logger import LOGGING
from db import elastic, redis


@asynccontextmanager
async def lifespan(app: FastAPI):
    await elastic.es.info()
    await redis.redis.initialize()
    yield
    redis.redis.close()
    elastic.es.close()


app = FastAPI(
    title=config.PROJECT_NAME,
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    lifespan=lifespan
)

app.include_router(films.router, prefix='/api/v1/films', tags=['films'])
app.include_router(persons.router, prefix='/api/v1/persons', tags=['persons'])
app.include_router(genres.router, prefix='/api/v1/genres', tags=['genres'])


if __name__ == '__main__':
    uvicorn.run(
        'main:app',
        host=config.FASTAPI_HOST,
        port=config.FASTAPI_PORT,
        log_config=LOGGING,
        log_level=logging.DEBUG,
    )
