import asyncio

import aioredis

from shared.logger import (
    get_child_logger,
)
from .app_utils import (
    async_repeat_until_cancelled,
    sleep,
)


async def redis_get_client(redis_uri):
    return await aioredis.create_redis(redis_uri)


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
