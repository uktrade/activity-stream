import asyncio

import aiohttp


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
            context.logger.error(
                'Raised exception in async_repeat_until_cancelled. '
                'Waiting %s seconds until looping.', exception_interval)
            context.raven_client.captureException()

            try:
                await asyncio.sleep(exception_interval)
            except asyncio.CancelledError:
                break


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
