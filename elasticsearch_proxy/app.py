import asyncio
import logging
import os
import sys

from aiohttp import web

from shared.utils import (
    normalise_environment,
)

ACCESS_LOG_FORMAT = '%a %t "%r" %s %b "%{Referer}i" "%{User-Agent}i" %{X-Forwarded-For}i'
LOGGER_NAME = 'activity-stream-elasticsearch-proxy'


async def run_application():
    app_logger = logging.getLogger(LOGGER_NAME)

    app_logger.debug('Examining environment...')
    env = normalise_environment(os.environ)
    port = env['PORT']
    app_logger.debug('Examining environment: done')

    async def handle(_):
        return web.json_response({})

    app_logger.debug('Creating listening web application...')
    app = web.Application()
    app.add_routes([web.get(r'/', handle)])

    runner = web.AppRunner(app, access_log_format=ACCESS_LOG_FORMAT)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    app_logger.debug('Creating listening web application: done')


def main():
    stdout_handler = logging.StreamHandler(sys.stdout)
    aiohttp_log = logging.getLogger('aiohttp.access')
    aiohttp_log.setLevel(logging.DEBUG)
    aiohttp_log.addHandler(stdout_handler)

    app_logger = logging.getLogger(LOGGER_NAME)
    app_logger.setLevel(logging.DEBUG)
    app_logger.addHandler(stdout_handler)

    loop = asyncio.get_event_loop()
    loop.create_task(run_application())
    loop.run_forever()


if __name__ == '__main__':
    main()
