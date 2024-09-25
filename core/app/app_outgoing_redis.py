import asyncio

import sentry_sdk

from .logger import (
    logged,
)
from .app_outgoing_utils import (
    repeat_until_cancelled,
)
from .utils import (
    get_child_context,
    sleep,
)


# So the latest URL for feeds don't hang around in Redis for
# ever if the feed is turned off
FEED_UPDATE_URL_EXPIRE = 60 * 60 * 24 * 31
NOT_EXISTS = b'__NOT_EXISTS__'
SHOW_FEED_AS_RED_IF_NO_REQUEST_IN_SECONDS = 20 * 60


async def acquire_and_keep_lock(parent_context, exception_intervals, key):
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
    context = get_child_context(parent_context, 'lock')
    logger = context.logger
    redis_client = context.redis_client
    ttl = 3
    aquire_interval = 0.5
    extend_interval = 0.5

    async def acquire():
        while True:
            logger.debug('Acquiring...')
            response = await redis_client.execute_command('SET', key, '1', 'EX', 1, 'NX')
            if response:
                logger.debug('Acquiring... (done)')
                break
            logger.debug('Acquiring... (failed)')
            await sleep(context, aquire_interval)

    async def extend_forever():
        await sleep(context, extend_interval)
        response = await redis_client.execute_command('EXPIRE', key, ttl)
        if not response:
            sentry_sdk.capture_message('Lock has been lost')
            await acquire()

    await acquire()
    asyncio.create_task(repeat_until_cancelled(
        context, exception_intervals, to_repeat=extend_forever,
    ))


async def get_feed_updates_url(context, feed_id):
    # For each live update, if a full ingest has recently finished, we use the URL set
    # by that (required, for example, if the endpoint has changed, or if lots of recent
    # event have been deleted). Otherwise, we use the URL set by the latest updates pass

    updates_seed_url_key = 'feed-updates-seed-url-' + feed_id
    updates_latest_url_key = 'feed-updates-latest-url-' + feed_id
    with logged(context.logger.debug, context.logger.warning, 'Getting updates url', []):
        while True:
            # We want the equivalent of an atomic GET/DEL, to avoid the race condition that the
            # full ingest sets the updates seed URL, but the updates chain then overwrites it
            # There is no atomic GETDEL command available, but it could be done with redis multi
            # exec, but, we have multiple concurrent usages of the redis client, which I suspect
            # would make transactions impossible right now
            # We do have an atomic GETSET however, so we use that with a special NOT_EXISTS value
            updates_seed_url = await context.redis_client.execute_command(
                'GETSET', updates_seed_url_key, NOT_EXISTS)
            context.logger.debug('Getting updates url... (seed: %s)', updates_seed_url)
            if updates_seed_url is not None and updates_seed_url != NOT_EXISTS:
                url = updates_seed_url
                break

            updates_latest_url = await context.redis_client.execute_command(
                'GET', updates_latest_url_key)
            context.logger.debug('Getting updates url... (latest: %s)', updates_latest_url)
            if updates_latest_url is not None:
                url = updates_latest_url
                break

            await sleep(context, 1)

    context.logger.debug('Getting updates url... (found: %s)', url)
    return url.decode('utf-8')


async def set_feed_updates_seed_url_init(context, feed_id):
    updates_seed_url_key = 'feed-updates-seed-url-' + feed_id
    with logged(context.logger.debug, context.logger.warning,
                'Setting updates seed url initial to (%s)', [NOT_EXISTS]):
        result = await context.redis_client.execute_command(
            'SET', updates_seed_url_key, NOT_EXISTS,
            'EX', FEED_UPDATE_URL_EXPIRE,
            'NX',
        )
        context.logger.debug('Setting updates seed url initial to (%s)... (result: %s)',
                             NOT_EXISTS, result)


async def set_feed_updates_seed_url(context, feed_id, updates_url):
    updates_seed_url_key = 'feed-updates-seed-url-' + feed_id
    with logged(context.logger.debug, context.logger.warning, 'Setting updates seed url to (%s)',
                [updates_url]):
        await context.redis_client.execute_command('SET', updates_seed_url_key, updates_url,
                                                   'EX', FEED_UPDATE_URL_EXPIRE)


async def set_feed_updates_url(context, feed_id, updates_url):
    updates_latest_url_key = 'feed-updates-latest-url-' + feed_id
    with logged(context.logger.debug, context.logger.warning, 'Setting updates url to (%s)',
                [updates_url]):
        await context.redis_client.execute_command('SET', updates_latest_url_key, updates_url,
                                                   'EX', FEED_UPDATE_URL_EXPIRE)


async def redis_set_metrics(context, metrics):
    with logged(context.logger.debug, context.logger.warning, 'Saving to Redis', []):
        await context.redis_client.execute_command('SET', 'metrics', metrics)


async def set_feed_status(context, feed_id, feed_max_interval, status):
    await context.redis_client.execute_command(
        'SET', feed_id + '-status', status,
        'EX', feed_max_interval + SHOW_FEED_AS_RED_IF_NO_REQUEST_IN_SECONDS)
