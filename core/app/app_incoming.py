import asyncio
import os

import aiohttp
from aiohttp import web
from prometheus_client import (
    CollectorRegistry,
)

from .dns import (
    AioHttpDnsResolver,
)
from .feeds import (
    parse_feed_config,
)
from .logger import (
    get_root_logger,
    logged,
)
from .metrics import (
    get_metrics,
)
from .raven import (
    get_raven_client,
)
from .app_incoming_server import (
    INCORRECT,
    authenticate_by_ip,
    authenticator,
    convert_errors_to_json,
    handle_get_metrics,
    handle_get_p1_check,
    handle_get_p2_check,
    handle_get_search_v2,
    raven_reporter,
    server_logger,
)
from .elasticsearch import (
    ALIAS_ACTIVITIES,
    ALIAS_OBJECTS,
)
from .redis import (
    redis_get_client,
)
from .utils import (
    Context,
    cancel_non_current_tasks,
    get_common_config,
    main,
    normalise_environment,
)
from . import settings

NONCE_EXPIRE = 120


async def run_incoming_application():
    logger = get_root_logger('incoming')

    with logged(logger.info, logger.warning, 'Examining environment', []):
        env = normalise_environment(os.environ)
        es_uri, es_version, es_aws_access_key_id, es_aws_secret_access_key, es_aws_region, \
            redis_uri, sentry = get_common_config(env)
        feeds = [parse_feed_config(feed) for feed in env['FEEDS']]
        port = env['PORT']
        incoming_key_pairs = [{
            'key_id': key_pair['KEY_ID'],
            'secret_key': key_pair['SECRET_KEY'],
            'permissions': key_pair['PERMISSIONS'],
        } for key_pair in env['INCOMING_ACCESS_KEY_PAIRS']]
        ip_whitelist = env['INCOMING_IP_WHITELIST']

    settings.ES_URI = es_uri
    settings.ES_VERSION = es_version
    settings.ES_AWS_ACCESS_KEY_ID = es_aws_access_key_id
    settings.ES_AWS_SECRET_ACCESS_KEY = es_aws_secret_access_key
    settings.ES_AWS_REGION = es_aws_region
    metrics_registry = CollectorRegistry()
    metrics = get_metrics(metrics_registry)
    conn = aiohttp.TCPConnector(use_dns_cache=False, resolver=AioHttpDnsResolver(metrics))
    session = aiohttp.ClientSession(
        connector=conn,
        headers={'Accept-Encoding': 'identity;q=1.0, *;q=0'},
    )
    redis_client = await redis_get_client(redis_uri)
    raven_client = get_raven_client(sentry, session, metrics)

    context = Context(
        logger=logger, metrics=metrics,
        raven_client=raven_client, redis_client=redis_client, session=session)

    with logged(context.logger.info, context.logger.warning, 'Creating listening web application',
                []):
        runner = await create_incoming_application(
            context, port, ip_whitelist, incoming_key_pairs, feeds,
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
        context, port, ip_whitelist, incoming_key_pairs, feeds):

    app = web.Application(middlewares=[
        server_logger(context.logger),
        convert_errors_to_json(),
        raven_reporter(context),
    ])

    private_app_v1 = web.Application(middlewares=[
        authenticate_by_ip(INCORRECT, ip_whitelist),
        authenticator(context, incoming_key_pairs, NONCE_EXPIRE),
    ])
    private_app_v1.add_routes([
        web.get(
            '/objects',
            # /objects v1 and v2 are identical
            handle_get_search_v2(context, ALIAS_OBJECTS)
        ),
    ])
    app.add_subapp('/v1/', private_app_v1)

    private_app_v2 = web.Application(middlewares=[
        authenticate_by_ip(INCORRECT, ip_whitelist),
        authenticator(context, incoming_key_pairs, NONCE_EXPIRE),
    ])
    private_app_v2.add_routes([
        web.get(
            '/activities',
            handle_get_search_v2(context, ALIAS_ACTIVITIES),
        ),
        web.get(
            '/objects',
            handle_get_search_v2(context, ALIAS_OBJECTS),
        ),
    ])
    app.add_subapp('/v2/', private_app_v2)

    private_app_v3 = web.Application(middlewares=[
        authenticate_by_ip(INCORRECT, ip_whitelist),
        authenticator(context, incoming_key_pairs, NONCE_EXPIRE),
    ])
    private_app_v3.add_routes([
        web.get(
            '/activities/_search',
            handle_get_search_v2(context, ALIAS_ACTIVITIES),
        ),
        web.get(
            '/objects/_search',
            handle_get_search_v2(context, ALIAS_OBJECTS),
        ),
        web.post(
            '/activities/_search',
            handle_get_search_v2(context, ALIAS_ACTIVITIES),
        ),
        web.post(
            '/objects/_search',
            handle_get_search_v2(context, ALIAS_OBJECTS),
        ),
    ])
    app.add_subapp('/v3/', private_app_v3)

    app.add_routes([
        web.get('/checks/p1', handle_get_p1_check(context)),
        web.get('/checks/p2', handle_get_p2_check(context, feeds)),
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
