import aioredis


async def redis_get_client(redis_uri):
    return await aioredis.create_redis_pool(redis_uri, minsize=3, maxsize=3)


async def set_nonce_nx(context, nonce_key, nonce_expire):
    return await context.redis_client.execute('SET', nonce_key, '1',
                                              'EX', nonce_expire, 'NX')
