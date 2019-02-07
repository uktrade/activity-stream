import asyncio
import functools

import raven
from raven.exceptions import (
    APIError,
    RateLimited,
)


def get_raven_client(sentry, session):
    return raven.Client(
        sentry['dsn'],
        environment=sentry['environment'],
        transport=functools.partial(QueuedAioHttpTransport, session=session))


class QueuedAioHttpTransport:
    # The official Raven asyncio client has assertions which fail on calling of its
    # close method, and has lots of code which we don't need

    is_async = True
    scheme = []

    def __init__(self, session):
        self.session = session
        self.queue_or_leak = leaky_queue()

    def async_send(self, url, data, headers, success_cb, failure_cb):
        async def _send():
            await send(self.session, url, data, headers, success_cb, failure_cb)
        asyncio.get_event_loop().create_task(self.queue_or_leak(_send))


def leaky_queue():
    lock = asyncio.Lock()

    async def _queue_or_leak(coroutine):
        if len(lock._waiters) < 100:
            async with lock:
                await coroutine()

    return _queue_or_leak


async def send(session, url, data, headers, success_cb, failure_cb):
    try:
        async with session.post(url, data=data, headers=headers) as response:
            message = response.headers.get('x-sentry-error', 'NO_ERROR_MESSAGE')
            retry_after = int(response.headers.get('retry-after', '0'))
            status = response.status
            if response.status == 200:
                success_cb()
            else:
                error = \
                    RateLimited(message, retry_after) if status == 429 else \
                    APIError(message, status)
                failure_cb(error)

    except asyncio.CancelledError:
        success_cb()
    except BaseException as exception:
        failure_cb(exception)
