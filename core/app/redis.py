import aioredis


async def redis_get_client(redis_uri):
    return await aioredis.create_redis_pool(redis_uri, minsize=3, maxsize=3)
