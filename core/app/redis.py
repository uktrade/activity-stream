import redis.asyncio as redis


async def redis_get_client(redis_uri):
    pool = redis.ConnectionPool.from_url(redis_uri)
    return redis.Redis.from_pool(pool)
