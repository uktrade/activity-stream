import asyncio
import functools
import logging
import signal
import sys

import aiohttp
import raven
from raven_aiohttp import QueuedAioHttpTransport

from shared.logger import (
    logged,
)


def flatten(to_flatten):
    return list(flatten_generator(to_flatten))


def flatten_generator(to_flatten):
    return (
        item
        for sub_list_or_generator in to_flatten
        for item in sub_list_or_generator
    )


async def async_repeat_until_cancelled(logger, raven_client, exception_intervals, coroutine):

    num_exceptions_in_chain = 0

    while True:
        try:
            await coroutine()
            num_exceptions_in_chain = 0
        except asyncio.CancelledError:
            break
        except BaseException:
            interval_index = min(num_exceptions_in_chain, len(exception_intervals) - 1)
            exception_interval = exception_intervals[interval_index]
            num_exceptions_in_chain += 1
            logger.exception(
                'Raised exception in async_repeat_until_cancelled. '
                'Waiting %s seconds until looping.', exception_interval)
            raven_client.captureException()

            try:
                await asyncio.sleep(exception_interval)
            except asyncio.CancelledError:
                break


def sub_dict_lower(super_dict, keys):
    return {
        key.lower(): super_dict[key]
        for key in keys
    }


async def cancel_non_current_tasks():
    current_task = asyncio.Task.current_task()
    all_tasks = asyncio.Task.all_tasks()
    non_current_tasks = [task for task in all_tasks if task != current_task]
    for task in non_current_tasks:
        task.cancel()
    # Allow CancelledException to be thrown at the location of all awaits
    await asyncio.sleep(0)


def get_raven_client(sentry):
    return raven.Client(
        sentry['dsn'],
        environment=sentry['environment'],
        transport=functools.partial(QueuedAioHttpTransport, workers=1, qsize=1000))


async def sleep(logger, interval):
    with logged(logger, 'Sleeping for %s seconds', [interval]):
        await asyncio.sleep(interval)


def http_429_retry_after(coroutine):

    async def _http_429_retry_after(*args, **kwargs):
        num_attempts = 0
        max_attempts = 10
        logger = kwargs['_http_429_retry_after_logger']

        while True:
            num_attempts += 1
            try:
                return await coroutine(*args, **kwargs)
            except aiohttp.ClientResponseError as client_error:
                if (num_attempts >= max_attempts or client_error.status != 429 or
                        'Retry-After' not in client_error.headers):
                    raise
                logger.debug('HTTP 429 received at attempt (%s). Will retry after (%s) seconds',
                             num_attempts, client_error.headers['Retry-After'])
                await asyncio.sleep(int(client_error.headers['Retry-After']))

    return _http_429_retry_after


def main(run_application_coroutine):
    stdout_handler = logging.StreamHandler(sys.stdout)
    app_logger = logging.getLogger('activity-stream')
    app_logger.setLevel(logging.DEBUG)
    app_logger.addHandler(stdout_handler)

    loop = asyncio.get_event_loop()
    cleanup = loop.run_until_complete(run_application_coroutine())

    async def cleanup_then_stop_loop():
        await cleanup()
        asyncio.get_event_loop().stop()
        return 'anything-to-avoid-pylint-assignment-from-none-error'

    cleanup_then_stop = cleanup_then_stop_loop()
    loop.add_signal_handler(signal.SIGINT, loop.create_task, cleanup_then_stop)
    loop.add_signal_handler(signal.SIGTERM, loop.create_task, cleanup_then_stop)
    loop.run_forever()
    app_logger.info('Reached end of main. Exiting now.')
