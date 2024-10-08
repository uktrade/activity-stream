async def redis_get_metrics(context):
    return await context.redis_client.execute_command('GET', 'metrics')


async def set_nonce_nx(context, nonce_key, nonce_expire):
    return await context.redis_client.execute_command('SET', nonce_key, '1',
                                                      'EX', nonce_expire, 'NX')


async def get_feeds_status(context, feed_ids):
    return await context.redis_client.execute_command('MGET', *[
        feed_id + '-status' for feed_id in feed_ids
    ])
