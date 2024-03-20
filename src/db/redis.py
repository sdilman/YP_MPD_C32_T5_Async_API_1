from redis.cluster import RedisCluster

redis: RedisCluster | None = None


# Функция понадобится при внедрении зависимостей
async def get_redis() -> RedisCluster:
    return redis
