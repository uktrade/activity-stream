import asyncio
import functools
import logging
import signal
import sys

import raven
from raven_aiohttp import QueuedAioHttpTransport

from shared.utils import (
    extract_keys,
)


def flatten(list_to_flatten):
    return [
        item
        for sublist in list_to_flatten
        for item in sublist
    ]


def async_repeat_until_cancelled(coroutine):

    async def _async_repeat_until_cancelled(*args, **kwargs):
        kwargs_to_pass, (raven_client, exception_interval, logger) = \
            extract_keys(kwargs, [
                '_async_repeat_until_cancelled_raven_client',
                '_async_repeat_until_cancelled_exception_interval',
                '_async_repeat_until_cancelled_logger',
            ])

        while True:
            try:
                await coroutine(*args, **kwargs_to_pass)
            except asyncio.CancelledError:
                break
            except BaseException:
                logger.exception(
                    'Raised exception in async_repeat_until_cancelled. '
                    'Waiting %s seconds until looping.', exception_interval)
                raven_client.captureException()

                try:
                    await asyncio.sleep(exception_interval)
                except asyncio.CancelledError:
                    break

    return _async_repeat_until_cancelled


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


def main(run_application_coroutine):
    stdout_handler = logging.StreamHandler(sys.stdout)
    aiohttp_log = logging.getLogger('aiohttp.access')
    aiohttp_log.setLevel(logging.DEBUG)
    aiohttp_log.addHandler(stdout_handler)

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
