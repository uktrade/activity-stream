import logging
import sys

from aiohttp import web


def run_application():
    async def handle(request):
        return web.Response(text='')

    app = web.Application()
    app.add_routes([web.get('/', handle)])
    access_log_format = '%a %t "%r" %s %b "%{Referer}i" "%{User-Agent}i" %{X-Forwarded-For}i'
    web.run_app(app, access_log_format=access_log_format)


def setup_logging():
    stdout_handler = logging.StreamHandler(sys.stdout)
    aiohttp_log = logging.getLogger('aiohttp.access')
    aiohttp_log.setLevel(logging.DEBUG)
    aiohttp_log.addHandler(stdout_handler)


setup_logging()
run_application()
