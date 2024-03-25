from redis.asyncio import Redis

from core import config

redis: Redis | None = Redis(host=config.REDIS_HOST, port=config.REDIS_PORT)


# Функция понадобится при внедрении зависимостей
async def get_redis() -> Redis:
    return redis
