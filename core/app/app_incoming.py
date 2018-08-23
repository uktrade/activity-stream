import asyncio
import os

import aiohttp
from aiohttp import web
from prometheus_client import (
    CollectorRegistry,
)

from shared.logger import (
    get_root_logger,
    logged,
)
from shared.utils import (
    get_common_config,
    normalise_environment,
)
from shared.web import (
    server_logger,
    authenticate_by_ip,
)

from .app_feeds import (
    parse_feed_config,
)
from .app_metrics import (
    get_metrics,
)
from .app_raven import (
    get_raven_client,
)
from .app_server import (
    INCORRECT,
    authenticator,
    authorizer,
    convert_errors_to_json,
    handle_get_check,
    handle_get_existing,
    handle_get_new,
    handle_get_metrics,
    handle_post,
    raven_reporter,
)
from .app_redis import (
    redis_get_client,
)
from .app_utils import (
    Context,
    cancel_non_current_tasks,
    main,
)

NONCE_EXPIRE = 120
PAGINATION_EXPIRE = 10


async def run_incoming_application():
    logger = get_root_logger('incoming')

    with logged(logger, 'Examining environment', []):
        env = normalise_environment(os.environ)
        es_endpoint, redis_uri, sentry = get_common_config(env)
        feed_endpoints = [parse_feed_config(feed) for feed in env['FEEDS']]
        port = env['PORT']
        incoming_key_pairs = [{
            'key_id': key_pair['KEY_ID'],
            'secret_key': key_pair['SECRET_KEY'],
            'permissions': key_pair['PERMISSIONS'],
        } for key_pair in env['INCOMING_ACCESS_KEY_PAIRS']]
        ip_whitelist = env['INCOMING_IP_WHITELIST']

    conn = aiohttp.TCPConnector(use_dns_cache=False, resolver=aiohttp.AsyncResolver())
    session = aiohttp.ClientSession(connector=conn, skip_auto_headers=['Accept-Encoding'])
    raven_client = get_raven_client(sentry, session)

    redis_client = await redis_get_client(redis_uri)

    metrics_registry = CollectorRegistry()
    metrics = get_metrics(metrics_registry)

    context = Context(
        logger=logger, metrics=metrics,
        raven_client=raven_client, redis_client=redis_client, session=session)

    with logged(context.logger, 'Creating listening web application', []):
        runner = await create_incoming_application(
            context, port, ip_whitelist, incoming_key_pairs,
            es_endpoint, feed_endpoints,
        )

    async def cleanup():
        await cancel_non_current_tasks()
        await runner.cleanup()

        redis_client.close()
        await redis_client.wait_closed()

        await session.close()
        # https://github.com/aio-libs/aiohttp/issues/1925
        await asyncio.sleep(0.250)

    return cleanup


async def create_incoming_application(
        context, port, ip_whitelist, incoming_key_pairs,
        es_endpoint, feed_endpoints):

    app = web.Application(middlewares=[
        server_logger(context.logger),
        convert_errors_to_json(),
        raven_reporter(context),
    ])

    private_app = web.Application(middlewares=[
        authenticate_by_ip(INCORRECT, ip_whitelist),
        authenticator(context, incoming_key_pairs, NONCE_EXPIRE),
        authorizer(),
    ])
    private_app.add_routes([
        web.post('/', handle_post),
        web.get(
            '/',
            handle_get_new(context, PAGINATION_EXPIRE, es_endpoint)
        ),
        web.get(
            '/{public_scroll_id}',
            handle_get_existing(context, PAGINATION_EXPIRE, es_endpoint),
            name='scroll',
        ),
    ])
    app.add_subapp('/v1/', private_app)
    app.add_routes([
        web.get('/check', handle_get_check(context, es_endpoint, feed_endpoints)),
        web.get('/metrics', handle_get_metrics(context)),
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
