import asyncio
import collections
import itertools
import json
import logging
import secrets
import signal
import string
import sys

import aiohttp

from .app_logger import (
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


def http_429_retry_after(coroutine):

    async def _http_429_retry_after(*args, **kwargs):
        num_attempts = 0
        max_attempts = 10
        logger = kwargs['_http_429_retry_after_context'].logger

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


def random_url_safe(count):
    return ''.join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(count))


def get_common_config(env):
    vcap_services = json.loads(env['VCAP_SERVICES'])
    es_uri = vcap_services['elasticsearch'][0]['credentials']['uri']
    redis_uri = vcap_services['redis'][0]['credentials']['uri']
    sentry = {
        'dsn': env['SENTRY_DSN'],
        'environment': env['SENTRY_ENVIRONMENT'],
    }
    return es_uri, redis_uri, sentry


def normalise_environment(key_values):
    ''' Converts denormalised dict of (string -> string) pairs, where the first string
        is treated as a path into a nested list/dictionary structure

        {
            "FOO__1__BAR": "setting-1",
            "FOO__1__BAZ": "setting-2",
            "FOO__2__FOO": "setting-3",
            "FOO__2__BAR": "setting-4",
            "FIZZ": "setting-5",
        }

        to the nested structure that this represents

        {
            "FOO": [{
                "BAR": "setting-1",
                "BAZ": "setting-2",
            }, {
                "BAR": "setting-3",
                "BAZ": "setting-4",
            }],
            "FIZZ": "setting-5",
        }

        If all the keys for that level parse as integers, then it's treated as a list
        with the actual keys only used for sorting

        This function is recursive, but it would be extremely difficult to hit a stack
        limit, and this function would typically by called once at the start of a
        program, so efficiency isn't too much of a concern.
    '''

    # Separator is chosen to
    # - show the structure of variables fairly easily;
    # - avoid problems, since underscores are usual in environment variables
    separator = '__'

    def get_first_component(key):
        return key.split(separator)[0]

    def get_later_components(key):
        return separator.join(key.split(separator)[1:])

    without_more_components = {
        key: value
        for key, value in key_values.items()
        if not get_later_components(key)
    }

    with_more_components = {
        key: value
        for key, value in key_values.items()
        if get_later_components(key)
    }

    def grouped_by_first_component(items):
        def by_first_component(item):
            return get_first_component(item[0])

        # groupby requires the items to be sorted by the grouping key
        return itertools.groupby(
            sorted(items, key=by_first_component),
            by_first_component,
        )

    def items_with_first_component(items, first_component):
        return {
            get_later_components(key): value
            for key, value in items
            if get_first_component(key) == first_component
        }

    nested_structured_dict = {
        **without_more_components, **{
            first_component: normalise_environment(
                items_with_first_component(items, first_component))
            for first_component, items in grouped_by_first_component(with_more_components.items())
        }}

    def all_keys_are_ints():
        def is_int(string_to_test):
            try:
                int(string_to_test)
                return True
            except ValueError:
                return False

        return all([is_int(key) for key, value in nested_structured_dict.items()])

    def list_sorted_by_int_key():
        return [
            value
            for key, value in sorted(
                nested_structured_dict.items(),
                key=lambda key_value: int(key_value[0])
            )
        ]

    return \
        list_sorted_by_int_key() if all_keys_are_ints() else \
        nested_structured_dict


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
