import asyncio
import logging
import os
import sys

import aiohttp
from aiohttp import web

POLLING_INTERVAL = 5


async def run_application():
    FEED_ENDPOINT = os.environ['FEED_ENDPOINT']
    SHARED_SECRET = os.environ['INTERNAL_API_SHARED_SECRET']
    feed_url = FEED_ENDPOINT + '?shared_secret=' + SHARED_SECRET

    async def handle(request):
        return web.Response(text='')

    app = web.Application()
    app.add_routes([web.get('/', handle)])
    access_log_format = '%a %t "%r" %s %b "%{Referer}i" "%{User-Agent}i" %{X-Forwarded-For}i'

    runner = web.AppRunner(app, access_log_format=access_log_format)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 8080)
    await site.start()

    async with aiohttp.ClientSession() as session:
        async for result in poll(session.get, feed_url):
            print(await result.content.read())


async def poll(async_func, *args, **kwargs):
    while True:
        yield await async_func(*args, **kwargs)
        await asyncio.sleep(POLLING_INTERVAL)


def setup_logging():
    stdout_handler = logging.StreamHandler(sys.stdout)
    aiohttp_log = logging.getLogger('aiohttp.access')
    aiohttp_log.setLevel(logging.DEBUG)
    aiohttp_log.addHandler(stdout_handler)


if __name__ == '__main__':
    setup_logging()

    loop = asyncio.get_event_loop()
    asyncio.ensure_future(run_application(), loop=loop)
    loop.run_forever()
