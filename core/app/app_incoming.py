import asyncio
import os

import aiohttp
from aiohttp import web
import aioredis
from shared.utils import (
    authenticate_by_ip,
    get_common_config,
    normalise_environment,
)

from .app_logger import (
    async_logger,
    get_logger_with_context,
    logged,
)
from .app_server import (
    INCORRECT,
    authenticator,
    authorizer,
    convert_errors_to_json,
    handle_get_existing,
    handle_get_new,
    handle_get_metrics,
    handle_post,
    server_logger,
    raven_reporter,
)
from .app_utils import (
    get_raven_client,
    cancel_non_current_tasks,
    main,
)

NONCE_EXPIRE = 120
PAGINATION_EXPIRE = 10


async def run_incoming_application():
    logger = get_logger_with_context('incoming')

    with logged(logger, 'Examining environment', []):
        env = normalise_environment(os.environ)
        es_endpoint, redis_uri, sentry = get_common_config(env)
        port = env['PORT']
        incoming_key_pairs = [{
            'key_id': key_pair['KEY_ID'],
            'secret_key': key_pair['SECRET_KEY'],
            'permissions': key_pair['PERMISSIONS'],
        } for key_pair in env['INCOMING_ACCESS_KEY_PAIRS']]
        ip_whitelist = env['INCOMING_IP_WHITELIST']

    raven_client = get_raven_client(sentry)
    session = aiohttp.ClientSession(skip_auto_headers=['Accept-Encoding'])
    redis_client = await aioredis.create_redis(redis_uri)

    runner = await create_incoming_application(
        logger, port, ip_whitelist, incoming_key_pairs,
        redis_client, raven_client, session, es_endpoint,
        _async_logger=logger,
        _async_logger_args=[],
    )

    async def cleanup():
        await cancel_non_current_tasks()
        await runner.cleanup()
        await raven_client.remote.get_transport().close()

        await session.close()
        # https://github.com/aio-libs/aiohttp/issues/1925
        await asyncio.sleep(0.250)

    return cleanup


@async_logger('Creating listening web application')
async def create_incoming_application(
        logger, port, ip_whitelist, incoming_key_pairs,
        redis_client, raven_client, session, es_endpoint, **_):

    app = web.Application(middlewares=[
        server_logger(logger),
        convert_errors_to_json(),
        raven_reporter(raven_client),
    ])

    private_app = web.Application(middlewares=[
        authenticate_by_ip(logger, INCORRECT, ip_whitelist),
        authenticator(incoming_key_pairs, redis_client, NONCE_EXPIRE),
        authorizer(),
    ])
    private_app.add_routes([
        web.post('/', handle_post),
        web.get('/', handle_get_new(session, redis_client, PAGINATION_EXPIRE, es_endpoint)),
        web.get(
            '/{public_scroll_id}',
            handle_get_existing(session, redis_client, PAGINATION_EXPIRE, es_endpoint),
            name='scroll',
        ),
    ])
    app.add_subapp('/v1/', private_app)
    app.add_routes([
        web.get('/metrics', handle_get_metrics(redis_client)),
    ])

    class NullAccessLogger(aiohttp.abc.AbstractAccessLogger):
        # pylint: disable=too-few-public-methods

        def log(self, request, response, time):
            pass

    runner = web.AppRunner(app, access_log_class=NullAccessLogger)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

    return runner


if __name__ == '__main__':
    main(run_incoming_application)
