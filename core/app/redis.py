import aioredis


async def redis_get_client(redis_uri):
    return aioredis.from_url(
        redis_uri,
        min_connections=3,
        max_connections=3,
        encoding='utf-8',
        decode_responses=True
    )
