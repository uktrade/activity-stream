import asyncio
import logging
import os
import sys

import aiohttp
from aiohttp import web

from shared.utils import (
    aws_auth_headers,
    get_common_config,
    normalise_environment,
)

ACCESS_LOG_FORMAT = '%a %t "%r" %s %b "%{Referer}i" "%{User-Agent}i" %{X-Forwarded-For}i'
LOGGER_NAME = 'activity-stream-elasticsearch-proxy'


async def run_application():
    app_logger = logging.getLogger(LOGGER_NAME)

    app_logger.debug('Examining environment...')
    env = normalise_environment(os.environ)
    port = env['PORT']
    es_endpoint, _, _ = get_common_config(env)
    app_logger.debug('Examining environment: done')

    session = aiohttp.ClientSession(skip_auto_headers=['Accept-Encoding'])

    async def handle(request):
        url = request.url.with_scheme(es_endpoint['protocol']) \
                         .with_host(es_endpoint['host']) \
                         .with_port(int(es_endpoint['port']))
        request_body = await request.read()
        source_headers = {
            header: request.headers[header]
            for header in ['Kbn-Version', 'Content-Type']
            if header in request.headers
        }
        auth_headers = aws_auth_headers(
            'es', es_endpoint, request.method, request.path,
            dict(request.query), source_headers, request_body,
        )
        response = await session.request(request.method, str(url), data=request_body,
                                         headers={**source_headers, **auth_headers})
        response_body = await response.read()
        return web.Response(status=response.status, body=response_body, headers=response.headers)

    app_logger.debug('Creating listening web application...')
    app = web.Application()
    app.add_routes([
        web.delete(r'/{path:.*}', handle),
        web.get(r'/{path:.*}', handle),
        web.post(r'/{path:.*}', handle),
        web.put(r'/{path:.*}', handle),
        web.head(r'/{path:.*}', handle),
    ])

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
