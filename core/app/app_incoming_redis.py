from . import settings

# So the latest URL for feeds don't hang around in Redis for
# ever if the feed is turned off
FEED_UPDATE_URL_EXPIRE = 60 * 60 * 24 * 31
NOT_EXISTS = b'__NOT_EXISTS__'
SHOW_FEED_AS_RED_IF_NO_REQUEST_IN_SECONDS = 10


async def set_private_scroll_id(context, public_scroll_id, private_scroll_id):
    await context.redis_client.execute('SET', f'private-scroll-id-{public_scroll_id}',
                                       private_scroll_id, 'EX', settings.PAGINATION_EXPIRE)


async def get_private_scroll_id(context, public_scroll_id):
    return await context.redis_client.execute('GET', f'private-scroll-id-{public_scroll_id}')


async def redis_get_metrics(context):
    return await context.redis_client.execute('GET', 'metrics')


async def set_nonce_nx(context, nonce_key):
    return await context.redis_client.execute('SET', nonce_key, '1',
                                              'EX', settings.NONCE_EXPIRE, 'NX')


async def set_feed_status(context, feed_id, feed_max_interval, status):
    await context.redis_client.execute(
        'SET', feed_id + '-status', status,
        'EX', feed_max_interval + SHOW_FEED_AS_RED_IF_NO_REQUEST_IN_SECONDS)


async def get_feeds_status(context, feed_ids):
    return await context.redis_client.execute('MGET', *[
        feed_id + '-status' for feed_id in feed_ids
    ])
