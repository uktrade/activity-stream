import asyncio
import collections
import logging
import signal
import sys

from shared.logger import (
    logged,
    get_child_logger,
)


Context = collections.namedtuple(
    'Context', ['logger', 'metrics', 'raven_client', 'redis_client', 'session'],
)


def get_child_context(context, name):
    return context._replace(logger=get_child_logger(context.logger, name))


def flatten(to_flatten):
    return list(flatten_generator(to_flatten))


def flatten_generator(to_flatten):
    return (
        item
        for sub_list_or_generator in to_flatten
        for item in sub_list_or_generator
    )


async def async_repeat_until_cancelled(context, exception_intervals, coroutine):

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
            context.logger.exception(
                'Raised exception in async_repeat_until_cancelled. '
                'Waiting %s seconds until looping.', exception_interval)
            context.raven_client.captureException()

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


async def sleep(context, interval):
    with logged(context.logger, 'Sleeping for %s seconds', [interval]):
        await asyncio.sleep(interval)


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
