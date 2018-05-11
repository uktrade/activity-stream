import asyncio
import logging
import sys

from aiohttp import web


async def run_application():
    async def handle(request):
        return web.Response(text='')

    app = web.Application()
    app.add_routes([web.get('/', handle)])
    access_log_format = '%a %t "%r" %s %b "%{Referer}i" "%{User-Agent}i" %{X-Forwarded-For}i'

    runner = web.AppRunner(app, access_log_format=access_log_format)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 8080)
    await site.start()


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
