import aioredis


async def redis_get_client(redis_uri):
    pool = aioredis.BlockingConnectionPool.from_url(redis_uri, max_connections=3)
    return aioredis.Redis(connection_pool=pool)
