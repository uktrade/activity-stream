import asyncio
import logging
import os
import sys
import urllib

import aiohttp
from aiohttp import web
from aiohttp_session import (
    get_session,
    session_middleware,
)
from aiohttp_session.redis_storage import RedisStorage
import aioredis

from shared.utils import (
    authenticate_by_ip,
    aws_auth_headers,
    get_common_config,
    normalise_environment,
)

ACCESS_LOG_FORMAT = '%a %t "%r" %s %b "%{Referer}i" "%{User-Agent}i" %{X-Forwarded-For}i'
INCORRECT = 'Incorrect authentication credentials.'
LOGGER_NAME = 'activity-stream-elasticsearch-proxy'


async def run_application():
    app_logger = logging.getLogger(LOGGER_NAME)

    app_logger.debug('Examining environment...')
    env = normalise_environment(os.environ)
    port = env['PORT']
    ip_whitelist = env['INCOMING_IP_WHITELIST']
    staff_sso_client_base = env['STAFF_SSO_BASE']
    staff_sso_client_id = env['STAFF_SSO_CLIENT_ID']
    staff_sso_client_secret = env['STAFF_SSO_CLIENT_SECRET']
    es_endpoint, redis_uri, _ = get_common_config(env)
    app_logger.debug('Examining environment: done')

    client_session = aiohttp.ClientSession(skip_auto_headers=['Accept-Encoding'])

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
        response = await client_session.request(request.method, str(url), data=request_body,
                                                headers={**source_headers, **auth_headers})
        response_body = await response.read()
        return web.Response(status=response.status, body=response_body, headers=response.headers)

    redis_pool = await aioredis.create_pool(redis_uri)
    redis_storage = RedisStorage(redis_pool, max_age=60*60*24)

    app_logger.debug('Creating listening web application...')
    app = web.Application(middlewares=[
        authenticate_by_ip(app_logger, INCORRECT, ip_whitelist),
        session_middleware(redis_storage),
        authenticate_by_staff_sso(client_session, staff_sso_client_base,
                                  staff_sso_client_id, staff_sso_client_secret),
    ])

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


def authenticate_by_staff_sso(client_session, base, client_id, client_secret):

    auth_path = '/o/authorize/'
    token_path = '/o/token/'
    me_path = '/api/v1/user/me/'
    grant_type = 'authorization_code'
    scope = 'read write'
    response_type = 'code'

    redirect_from_sso_path = '/__redirect_from_sso'
    session_token_key = 'staff_sso_access_token'

    def get_redirect_uri_authenticate(request):
        state = 'should-be-more-unique'
        redirect_uri_callback = urllib.parse.quote(get_redirect_uri_callback(request), safe='')
        return f'{base}{auth_path}?' \
               f'scope={scope}&state={state}&' \
               f'redirect_uri={redirect_uri_callback}&' \
               f'response_type={response_type}&' \
               f'client_id={client_id}'

    def get_redirect_uri_callback(request):
        uri = request.url.with_scheme(request.headers['X-Forwarded-Proto']) \
                         .with_path(redirect_from_sso_path) \
                         .with_query({})
        return str(uri)

    def get_redirect_uri_final(_):
        return '/'

    @web.middleware
    async def _authenticate_by_sso(request, handler):
        session = await get_session(request)

        if request.path != redirect_from_sso_path and session_token_key not in session:
            return web.Response(status=302, headers={
                'Location': get_redirect_uri_authenticate(request),
            })

        if request.path == redirect_from_sso_path:
            code = request.query['code']
            sso_response = await client_session.post(
                f'{base}{token_path}',
                data={
                    'grant_type': grant_type,
                    'code': code,
                    'client_id': client_id,
                    'client_secret': client_secret,
                    'redirect_uri': get_redirect_uri_callback(request),
                },
            )
            session[session_token_key] = (await sso_response.json())['access_token']
            return web.Response(status=302, headers={'Location': get_redirect_uri_final(request)})

        token = session[session_token_key]
        me_response = await client_session.get(f'{base}{me_path}', headers={
            'Authorization': f'Bearer {token}'
        })
        # Without this, suspect connections are left open leading to eventual deadlock
        await me_response.read()
        return \
            await handler(request) if me_response.status == 200 else \
            web.Response(status=302, headers={'Location': get_redirect_uri_authenticate(request)})

    return _authenticate_by_sso


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