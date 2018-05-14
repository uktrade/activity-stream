import asyncio
import json
import logging
import os
import sys

import aiohttp
from aiohttp import web
from lxml import etree

POLLING_INTERVAL = 5


async def run_application():
    FEED_ENDPOINT = os.environ['FEED_ENDPOINT']
    ELASTIC_SEARCH_ENDPOINT = os.environ['ELASTIC_SEARCH_ENDPOINT']
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
            feed_contents = await result.content.read()
            es_bulk_contents = es_bulk(feed_contents)
            await session.post(ELASTIC_SEARCH_ENDPOINT + '_bulk', data=es_bulk_contents)


async def poll(async_func, *args, **kwargs):
    while True:
        yield await async_func(*args, **kwargs)
        await asyncio.sleep(POLLING_INTERVAL)


def es_bulk(feed_xml):
    feed = etree.XML(feed_xml)
    return '\n'.join(flatten([
        [json.dumps(contents['action_and_metadata']), json.dumps(contents['source'])]
        for es_bulk in feed.iter('{http://trade.gov.uk/activity-stream/v1}elastic_search_bulk')
        for contents in [json.loads(es_bulk.text)]
    ])) + '\n'


def flatten(l):
    return [item for sublist in l for item in sublist]


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
