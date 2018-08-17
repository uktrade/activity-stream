import asyncio

import aioredis

from shared.logger import (
    get_child_logger,
    logged,
)
from .app_utils import (
    async_repeat_until_cancelled,
    sleep,
)

# So the latest URL for feeds don't hang around in Redis for
# ever if the feed is turned off
FEED_UPDATE_URL_EXPIRE = 60 * 60 * 24 * 31
NOT_EXISTS = b'__NOT_EXISTS__'


async def redis_get_client(redis_uri):
    return await aioredis.create_redis_pool(redis_uri, minsize=3, maxsize=3)


async def set_private_scroll_id(redis_client, public_scroll_id, private_scroll_id, expire):
    await redis_client.execute('SET', f'private-scroll-id-{public_scroll_id}',
                               private_scroll_id, 'EX', expire)


async def get_private_scroll_id(redis_client, public_scroll_id):
    return await redis_client.execute('GET', f'private-scroll-id-{public_scroll_id}')


async def acquire_and_keep_lock(parent_logger, redis_client, raven_client, exception_intervals,
                                key):
    ''' Prevents Elasticsearch errors during deployments

    The exceptions would be caused by a new deployment deleting indexes while
    the previous deployment is still ingesting into them

    We do not offer a delete for simplicity: the lock will just expire if it's
    not extended, which happens on destroy of the application

    We don't use Redlock, since we don't care too much if a Redis failure causes
    multiple clients to have the lock for a period of time. It would only cause
    Elasticsearch errors to appear in sentry, but otherwise there would be no
    harm
    '''
    logger = get_child_logger(parent_logger, 'lock')
    ttl = 3
    aquire_interval = 0.5
    extend_interval = 0.5

    async def acquire():
        while True:
            logger.debug('Acquiring...')
            response = await redis_client.execute('SET', key, '1', 'EX', ttl, 'NX')
            if response == b'OK':
                logger.debug('Acquiring... (done)')
                break
            logger.debug('Acquiring... (failed)')
            await sleep(logger, aquire_interval)

    async def extend_forever():
        await sleep(logger, extend_interval)
        response = await redis_client.execute('EXPIRE', key, ttl)
        if response != 1:
            raven_client.captureMessage('Lock has been lost')
            await acquire()

    await acquire()
    asyncio.get_event_loop().create_task(async_repeat_until_cancelled(
        logger, raven_client, exception_intervals, extend_forever,
    ))


async def get_feed_updates_url(logger, redis_client, feed_id):
    # For each live update, if a full ingest has recently finished, we use the URL set
    # by that (required, for example, if the endpoint has changed, or if lots of recent
    # event have been deleted). Otherwise, we use the URL set by the latest updates pass

    updates_seed_url_key = 'feed-updates-seed-url-' + feed_id
    updates_latest_url_key = 'feed-updates-latest-url-' + feed_id
    with logged(logger, 'Getting updates url', []):
        while True:
            # We want the equivalent of an atomic GET/DEL, to avoid the race condition that the
            # full ingest sets the updates seed URL, but the updates chain then overwrites it
            # There is no atomic GETDEL command available, but it could be done with redis multi
            # exec, but, we have multiple concurrent usages of the redis client, which I suspect
            # would make transactions impossible right now
            # We do have an atomic GETSET however, so we use that with a special NOT_EXISTS value
            updates_seed_url = await redis_client.execute('GETSET', updates_seed_url_key,
                                                          NOT_EXISTS)
            logger.debug('Getting updates url... (seed: %s)', updates_seed_url)
            if updates_seed_url is not None and updates_seed_url != NOT_EXISTS:
                url = updates_seed_url
                break

            updates_latest_url = await redis_client.execute('GET', updates_latest_url_key)
            logger.debug('Getting updates url... (latest: %s)', updates_latest_url)
            if updates_latest_url is not None:
                url = updates_latest_url
                break

            await sleep(logger, 1)

    logger.debug('Getting updates url... (found: %s)', url)
    return url.decode('utf-8')


async def set_feed_updates_seed_url_init(logger, redis_client, feed_id):
    updates_seed_url_key = 'feed-updates-seed-url-' + feed_id
    with logged(logger, 'Setting updates seed url initial to (%s)', [NOT_EXISTS]):
        result = await redis_client.execute('SET', updates_seed_url_key, NOT_EXISTS,
                                            'EX', FEED_UPDATE_URL_EXPIRE,
                                            'NX')
        logger.debug('Setting updates seed url initial to (%s)... (result: %s)',
                     NOT_EXISTS, result)


async def set_feed_updates_seed_url(logger, redis_client, feed_id, updates_url):
    updates_seed_url_key = 'feed-updates-seed-url-' + feed_id
    with logged(logger, 'Setting updates seed url to (%s)', [updates_url]):
        await redis_client.execute('SET', updates_seed_url_key, updates_url,
                                   'EX', FEED_UPDATE_URL_EXPIRE)


async def set_feed_updates_url(logger, redis_client, feed_id, updates_url):
    updates_latest_url_key = 'feed-updates-latest-url-' + feed_id
    with logged(logger, 'Setting updates url to (%s)', [updates_url]):
        await redis_client.execute('SET', updates_latest_url_key, updates_url,
                                   'EX', FEED_UPDATE_URL_EXPIRE)


async def redis_set_metrics(logger, redis_client, metrics):
    with logged(logger, 'Saving to Redis', []):
        await redis_client.execute('SET', 'metrics', metrics)


async def redis_get_metrics(redis_client):
    return await redis_client.execute('GET', 'metrics')


async def set_nonce_nx(redis_client, nonce_key, nonce_expire):
    return await redis_client.execute('SET', nonce_key, '1',
                                      'EX', nonce_expire, 'NX')
