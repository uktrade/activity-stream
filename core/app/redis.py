import aioredis


async def redis_get_client(redis_uri):
    redis = aioredis.from_url(
        redis_uri,
        encoding='utf-8',
        decode_responses=True
    )
    return redis.client()
