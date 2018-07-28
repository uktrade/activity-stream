import asyncio
import functools
import json
import logging
import os
import signal
import sys

import aiohttp
from aiohttp import web
from prometheus_client import (
    CollectorRegistry,
)
from raven import Client
from raven_aiohttp import QueuedAioHttpTransport

from .app_elasticsearch import (
    es_bulk,
    create_indexes,
    create_mappings,
    get_new_index_names,
    get_old_index_names,
    indexes_matching_feeds,
    indexes_matching_no_feeds,
    add_remove_aliases_atomically,
    delete_indexes,
    refresh_indexes,
)

from .app_feeds import (
    ActivityStreamFeed,
    ZendeskFeed,
)
from .app_metrics import (
    async_inprogress,
    async_timer,
    get_metrics,
)
from .app_server import (
    authenticator,
    authorizer,
    convert_errors_to_json,
    handle_get_existing,
    handle_get_new,
    handle_get_metrics,
    handle_post,
    raven_reporter,
)
from .app_utils import (
    ExpiringDict,
    normalise_environment,
    repeat_while,
)

EXCEPTION_INTERVAL = 60
NONCE_EXPIRE = 120


async def run_application():
    app_logger = logging.getLogger('activity-stream')

    app_logger.debug('Examining environment...')
    env = normalise_environment(os.environ)

    port = env['PORT']

    feed_endpoints = [parse_feed_config(feed) for feed in env['FEEDS']]

    incoming_key_pairs = [{
        'key_id': key_pair['KEY_ID'],
        'secret_key': key_pair['SECRET_KEY'],
        'permissions': key_pair['PERMISSIONS'],
    } for key_pair in env['INCOMING_ACCESS_KEY_PAIRS']]
    ip_whitelist = env['INCOMING_IP_WHITELIST']

    es_endpoint = {
        'host': env['ELASTICSEARCH']['HOST'],
        'access_key_id': env['ELASTICSEARCH']['AWS_ACCESS_KEY_ID'],
        'secret_key': env['ELASTICSEARCH']['AWS_SECRET_ACCESS_KEY'],
        'region': env['ELASTICSEARCH']['REGION'],
        'protocol': env['ELASTICSEARCH']['PROTOCOL'],
        'base_url': (
            env['ELASTICSEARCH']['PROTOCOL'] + '://' +
            env['ELASTICSEARCH']['HOST'] + ':' + env['ELASTICSEARCH']['PORT']
        ),
        'port': env['ELASTICSEARCH']['PORT'],
    }

    sentry_dsn = env['SENTRY_DSN']
    sentry_environment = env['SENTRY_ENVIRONMENT']

    app_logger.debug('Examining environment: done')

    raven_client = Client(
        dns=sentry_dsn,
        environment=sentry_environment,
        transport=functools.partial(QueuedAioHttpTransport, workers=1, qsize=1000))
    session = aiohttp.ClientSession(skip_auto_headers=['Accept-Encoding'])
    running = True

    metrics_registry = CollectorRegistry()
    await create_outgoing_application(
        lambda: running, get_metrics(metrics_registry), raven_client, session,
        feed_endpoints, es_endpoint,
    )
    runner = await create_incoming_application(
        port, ip_whitelist, incoming_key_pairs, metrics_registry,
        raven_client, session, es_endpoint,
    )

    async def cleanup():
        nonlocal running
        running = False
        await runner.cleanup()
        await raven_client.remote.get_transport().close()
        await session.close()

    return cleanup


async def create_incoming_application(port, ip_whitelist, incoming_key_pairs,
                                      metrics_registry, raven_client, session, es_endpoint):
    app_logger = logging.getLogger('activity-stream')

    app_logger.debug('Creating listening web application...')

    public_to_private_scroll_ids = ExpiringDict(30)
    app = web.Application(middlewares=[
        convert_errors_to_json(),
        raven_reporter(raven_client),
    ])
    private_app = web.Application(middlewares=[
        authenticator(ip_whitelist, incoming_key_pairs, NONCE_EXPIRE),
        authorizer(),
    ])
    private_app.add_routes([
        web.post('/', handle_post),
        web.get('/', handle_get_new(session, public_to_private_scroll_ids, es_endpoint)),
        web.get(
            '/{public_scroll_id}',
            handle_get_existing(session, public_to_private_scroll_ids, es_endpoint), name='scroll',
        ),
    ])
    app.add_subapp('/v1/', private_app)
    app.add_routes([
        web.get('/metrics', handle_get_metrics(metrics_registry)),
    ])
    access_log_format = '%a %t "%r" %s %b "%{Referer}i" "%{User-Agent}i" %{X-Forwarded-For}i'

    runner = web.AppRunner(app, access_log_format=access_log_format)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    app_logger.debug('Creating listening web application: done')

    return runner


async def create_outgoing_application(is_running, metrics, raven_client, session,
                                      feed_endpoints, es_endpoint):
    asyncio.ensure_future(repeat_while(
        functools.partial(ingest_feeds, is_running, metrics, session, feed_endpoints, es_endpoint),
        predicate=is_running, raven_client=raven_client,
        exception_interval=EXCEPTION_INTERVAL, logging_title='Polling feed'))


async def ingest_feeds(is_running, metrics, session, feed_endpoints, es_endpoint):
    all_feed_ids = feed_unique_ids(feed_endpoints)
    new_index_names = get_new_index_names(all_feed_ids)
    old_index_names = await get_old_index_names(session, es_endpoint)

    await delete_indexes(session, es_endpoint, old_index_names['without-alias'])
    await create_indexes(session, es_endpoint, new_index_names)
    await create_mappings(session, es_endpoint, new_index_names)

    ingest_results = await asyncio.gather(*[
        ingest_feed(
            is_running, metrics, session, feed_endpoint, es_endpoint, new_index_names[i],
            _async_timer=metrics['ingest_feed_duration_seconds'],
            _async_timer_labels=[feed_endpoint.unique_id],
            _async_timer_is_running=is_running,
            _async_inprogress=metrics['ingest_inprogress_ingests_total'],
        )
        for i, feed_endpoint in enumerate(feed_endpoints)
    ], return_exceptions=True)

    successful_feed_ids = feed_unique_ids([
        feed_endpoint
        for i, feed_endpoint in enumerate(feed_endpoints)
        if not isinstance(ingest_results[i], BaseException)
    ])

    indexes_to_add_to_alias = indexes_matching_feeds(new_index_names, successful_feed_ids)
    indexes_to_remove_from_alias = \
        indexes_matching_feeds(old_index_names['with-alias'], successful_feed_ids) + \
        indexes_matching_no_feeds(old_index_names['with-alias'], all_feed_ids)

    await refresh_indexes(session, es_endpoint, new_index_names)
    await add_remove_aliases_atomically(session, es_endpoint,
                                        indexes_to_add_to_alias, indexes_to_remove_from_alias)


def feed_unique_ids(feed_endpoints):
    return [feed_endpoint.unique_id for feed_endpoint in feed_endpoints]


@async_inprogress
@async_timer
async def ingest_feed(is_running, metrics, session, feed_endpoint, es_endpoint, index_name, **_):
    async for es_bulk_items in poll(is_running, metrics, session, feed_endpoint, index_name):
        await es_bulk(
            session, es_endpoint, es_bulk_items,
            _async_counter=metrics['ingest_activities_nonunique_total'],
            _async_counter_labels=[feed_endpoint.unique_id],
            _async_counter_increment_by=len(es_bulk_items),
        )


async def poll(is_running, metrics, session, feed, index_name):
    app_logger = logging.getLogger('activity-stream')

    href = feed.seed
    while href:
        app_logger.debug('Polling')
        feed_contents = await get_feed_contents(
            session, href, feed.auth_headers(href),
            _async_timer=metrics['ingest_page_duration_seconds'],
            _async_timer_labels=[feed.unique_id, 'pull'],
            _async_timer_is_running=is_running,
        )

        app_logger.debug('Parsing JSON...')
        feed_parsed = json.loads(feed_contents)
        app_logger.debug('Parsed')

        yield feed.convert_to_bulk_es(feed_parsed, index_name)

        app_logger.debug('Finding next URL...')
        href = feed.next_href(feed_parsed)
        app_logger.debug('Finding next URL: done (%s)', href)

        interval, message = \
            (feed.polling_page_interval, 'Will poll next page in feed') if href else \
            (feed.polling_seed_interval, 'Will poll seed page')

        app_logger.debug(message)
        app_logger.debug('Sleeping for %s seconds', interval)

        await asyncio.sleep(interval)


@async_timer
async def get_feed_contents(session, href, headers, **_):
    app_logger = logging.getLogger('activity-stream')

    app_logger.debug('Fetching feed...')
    result = await session.get(href, headers=headers)
    app_logger.debug('Fetching feed: done')

    if result.status != 200:
        raise Exception(await result.text())

    app_logger.debug('Fetching feed contents...')
    contents = await result.content.read()
    app_logger.debug('Fetched feed contents: done')

    return contents


def parse_feed_config(feed_config):
    by_feed_type = {
        'activity_stream': ActivityStreamFeed,
        'zendesk': ZendeskFeed,
    }
    return by_feed_type[feed_config['TYPE']].parse_config(feed_config)


def main():
    stdout_handler = logging.StreamHandler(sys.stdout)
    aiohttp_log = logging.getLogger('aiohttp.access')
    aiohttp_log.setLevel(logging.DEBUG)
    aiohttp_log.addHandler(stdout_handler)

    app_logger = logging.getLogger('activity-stream')
    app_logger.setLevel(logging.DEBUG)
    app_logger.addHandler(stdout_handler)

    loop = asyncio.get_event_loop()
    cleanup = loop.run_until_complete(run_application())

    async def cleanup_then_stop_loop():
        await cleanup()
        asyncio.get_event_loop().stop()
        return 'anything-to-avoid-pylint-assignment-from-none-error'

    cleanup_then_stop = cleanup_then_stop_loop()
    loop.add_signal_handler(signal.SIGINT, asyncio.ensure_future, cleanup_then_stop)
    loop.add_signal_handler(signal.SIGTERM, asyncio.ensure_future, cleanup_then_stop)
    loop.run_forever()
    app_logger.info('Reached end of main. Exiting now.')


if __name__ == '__main__':
    main()
