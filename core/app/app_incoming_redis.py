from . import settings


async def set_private_scroll_id(context, public_scroll_id, private_scroll_id):
    await context.redis_client.execute('SET', f'private-scroll-id-{public_scroll_id}',
                                       private_scroll_id, 'EX', settings.PAGINATION_EXPIRE)


async def get_private_scroll_id(context, public_scroll_id):
    return await context.redis_client.execute('GET', f'private-scroll-id-{public_scroll_id}')


async def redis_get_metrics(context):
    return await context.redis_client.execute('GET', 'metrics')


async def set_nonce_nx(context, nonce_key, nonce_expire):
    return await context.redis_client.execute('SET', nonce_key, '1',
                                              'EX', nonce_expire, 'NX')


async def get_feeds_status(context, feed_ids):
    return await context.redis_client.execute('MGET', *[
        feed_id + '-status' for feed_id in feed_ids
    ])
