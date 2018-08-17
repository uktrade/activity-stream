import aioredis


async def redis_get_client(redis_uri):
    return await aioredis.create_redis(redis_uri)
