import asyncio
import functools
import json
import logging
import os
import signal
import sys

import aiohttp
from aiohttp import web
from raven import Client
from raven_aiohttp import QueuedAioHttpTransport

from .app_elasticsearch import (
    es_bulk,
    create_indexes,
    create_mappings,
    get_new_index_names,
    get_old_index_names,
    set_alias,
    delete_indexes,
    refresh_indexes,
)

from .app_feeds import (
    ActivityStreamFeed,
    ZendeskFeed,
)
from .app_server import (
    authenticator,
    authorizer,
    convert_errors_to_json,
    handle_get_existing,
    handle_get_new,
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

    def is_running():
        nonlocal running
        return running

    await create_outgoing_application(
        is_running, raven_client, session, feed_endpoints, es_endpoint,
    )
    runner = await create_incoming_application(
        port, ip_whitelist, incoming_key_pairs, raven_client, session, es_endpoint,
    )

    async def cleanup():
        nonlocal running
        running = False
        await runner.cleanup()
        await raven_client.remote.get_transport().close()
        await session.close()

    return cleanup


async def create_incoming_application(port, ip_whitelist, incoming_key_pairs,
                                      raven_client, session, es_endpoint):
    app_logger = logging.getLogger('activity-stream')

    app_logger.debug('Creating listening web application...')

    public_to_private_scroll_ids = ExpiringDict(30)
    app = web.Application(middlewares=[
        convert_errors_to_json(),
        raven_reporter(raven_client),
        authenticator(ip_whitelist, incoming_key_pairs, NONCE_EXPIRE),
        authorizer(),
    ])
    app.add_routes([
        web.post('/v1/', handle_post),
        web.get('/v1/', handle_get_new(session, public_to_private_scroll_ids, es_endpoint)),
        web.get(
            '/v1/{public_scroll_id}',
            handle_get_existing(session, public_to_private_scroll_ids, es_endpoint), name='scroll',
        ),
    ])
    access_log_format = '%a %t "%r" %s %b "%{Referer}i" "%{User-Agent}i" %{X-Forwarded-For}i'

    runner = web.AppRunner(app, access_log_format=access_log_format)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    app_logger.debug('Creating listening web application: done')

    return runner


async def create_outgoing_application(is_running, raven_client, session,
                                      feed_endpoints, es_endpoint):
    asyncio.ensure_future(repeat_while(
        functools.partial(ingest_feeds, session, feed_endpoints, es_endpoint),
        predicate=is_running, raven_client=raven_client,
        exception_interval=EXCEPTION_INTERVAL, logging_title='Polling feed'))


async def ingest_feeds(session, feed_endpoints, es_endpoint):
    new_index_names = get_new_index_names(feed_unique_ids(feed_endpoints))
    old_index_names = await get_old_index_names(session, es_endpoint)

    await delete_indexes(session, es_endpoint, old_index_names)
    await create_indexes(session, es_endpoint, new_index_names)
    await create_mappings(session, es_endpoint, new_index_names)

    await asyncio.gather(*[
        ingest_feed(session, feed_endpoint, es_endpoint, new_index_names[i])
        for i, feed_endpoint in enumerate(feed_endpoints)
    ])

    await refresh_indexes(session, es_endpoint, new_index_names)
    await set_alias(session, es_endpoint, new_index_names)


def feed_unique_ids(feed_endpoints):
    return [feed_endpoint.unique_id for feed_endpoint in feed_endpoints]


async def ingest_feed(session, feed_endpoint, es_endpoint, index_name):
    async for feed in poll(session, feed_endpoint, index_name):
        await es_bulk(session, es_endpoint, feed)


async def poll(session, feed, index_name):
    app_logger = logging.getLogger('activity-stream')

    href = feed.seed
    while href:
        app_logger.debug('Polling')
        result = await session.get(href, headers=feed.auth_headers(href))

        app_logger.debug('Fetching contents of feed...')
        feed_contents = await result.content.read()
        app_logger.debug('Fetching contents of feed: done (%s)', feed_contents)

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
