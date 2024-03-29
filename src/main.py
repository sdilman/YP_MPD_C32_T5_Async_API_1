import logging.config
from contextlib import asynccontextmanager

from fastapi import FastAPI

from api.v1 import films, persons, genres
from core import config
from core.logger import LOGGING
from db import elastic, redis
from fastapi_pagination import add_pagination


@asynccontextmanager
async def lifespan(app: FastAPI):
    await elastic.es.info()
    await redis.redis.initialize()
    logging.config.dictConfig(LOGGING)
    yield
    redis.redis.close()
    elastic.es.close()


app = FastAPI(
    title=config.PROJECT_NAME,
    docs_url='/api/openapi',
    openapi_url='/api/openapi.json',
    lifespan=lifespan
)

add_pagination(app)
app.include_router(films.router, prefix='/api/v1/films', tags=['films'])
app.include_router(persons.router, prefix='/api/v1/persons', tags=['persons'])
app.include_router(genres.router, prefix='/api/v1/genres', tags=['genres'])
