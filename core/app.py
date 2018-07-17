import asyncio
import functools
import json
import logging
import os
import signal
import sys

import aiohttp
from aiohttp import web

from .app_elasticsearch import (
    es_bulk,
    ensure_index,
    ensure_mappings,
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
)
from .app_utils import (
    ExpiringDict,
    normalise_environment,
    repeat_even_on_exception,
)

EXCEPTION_INTERVAL = 60
NONCE_EXPIRE = 120


async def run_application():
    app_logger = logging.getLogger(__name__)

    app_logger.debug('Examining environment...')
    env = normalise_environment(os.environ)

    port = env['PORT']

    def parse_feed_config(feed_config):
        by_feed_type = {
            'activity_stream': ActivityStreamFeed,
            'zendesk': ZendeskFeed,
        }
        return by_feed_type[feed_config['TYPE']].parse_config(feed_config)

    feed_endpoints = [parse_feed_config(feed) for feed in env['FEEDS']]

    incoming_key_pairs = [{
        'key_id': key_pair['KEY_ID'],
        'secret_key': key_pair['SECRET_KEY'],
        'permissions': key_pair['PERMISSIONS'],
    } for key_pair in env['INCOMING_ACCESS_KEY_PAIRS']]
    ip_whitelist = env['INCOMING_IP_WHITELIST']

    es_host = env['ELASTICSEARCH']['HOST']
    es_endpoint = {
        'host': es_host,
        'access_key_id': env['ELASTICSEARCH']['AWS_ACCESS_KEY_ID'],
        'secret_key': env['ELASTICSEARCH']['AWS_SECRET_ACCESS_KEY'],
        'region': env['ELASTICSEARCH']['REGION'],
        'protocol': env['ELASTICSEARCH']['PROTOCOL'],
        'base_url': (
            env['ELASTICSEARCH']['PROTOCOL'] + '://' +
            es_host + ':' + env['ELASTICSEARCH']['PORT']
        ),
        'port': env['ELASTICSEARCH']['PORT'],
    }

    app_logger.debug('Examining environment: done')

    async with aiohttp.ClientSession() as session:
        await ensure_index(session, es_endpoint)
        await ensure_mappings(session, es_endpoint)
        await create_incoming_application(
            port, ip_whitelist, incoming_key_pairs, session, es_endpoint,
        )
        feeds = await create_outgoing_application(
            session, feed_endpoints, es_endpoint,
        )
        await feeds


async def create_incoming_application(port, ip_whitelist, incoming_key_pairs,
                                      session, es_endpoint):
    app_logger = logging.getLogger(__name__)

    app_logger.debug('Creating listening web application...')

    public_to_private_scroll_ids = ExpiringDict(30)
    app = web.Application(middlewares=[
        convert_errors_to_json(),
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


async def create_outgoing_application(session, feed_endpoints, es_endpoint):
    feeds = [
        repeat_even_on_exception(functools.partial(
            ingest_feed,
            session, feed_endpoint,
            es_endpoint,
        ), exception_interval=EXCEPTION_INTERVAL, logging_title='Polling feed')
        for feed_endpoint in feed_endpoints
    ]
    return asyncio.gather(*feeds)


async def ingest_feed(session, feed_endpoint, es_endpoint):
    async for feed in poll(session, feed_endpoint):
        await es_bulk(session, es_endpoint, feed)


async def poll(session, feed):
    app_logger = logging.getLogger(__name__)

    href = feed.seed
    while True:
        app_logger.debug('Polling')
        result = await session.get(href, headers=feed.auth_headers(href))

        app_logger.debug('Fetching contents of feed...')
        feed_contents = await result.content.read()
        app_logger.debug('Fetching contents of feed: done (%s)', feed_contents)

        app_logger.debug('Parsing JSON...')
        feed_parsed = json.loads(feed_contents)
        app_logger.debug('Parsed')

        yield feed.convert_to_bulk_es(feed_parsed)

        app_logger.debug('Finding next URL...')
        href = feed.next_href(feed_parsed)
        app_logger.debug('Finding next URL: done (%s)', href)

        href, interval, message = \
            (href, feed.polling_page_interval, 'Will poll next page in feed') if href else \
            (feed.seed, feed.polling_seed_interval, 'Will poll seed page')

        app_logger.debug(message)
        app_logger.debug('Sleeping for %s seconds', interval)
        await asyncio.sleep(interval)


def setup_logging():
    stdout_handler = logging.StreamHandler(sys.stdout)
    aiohttp_log = logging.getLogger('aiohttp.access')
    aiohttp_log.setLevel(logging.DEBUG)
    aiohttp_log.addHandler(stdout_handler)

    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(logging.DEBUG)
    app_logger.addHandler(stdout_handler)


def exit_gracefully():
    asyncio.get_event_loop().stop()
    sys.exit(0)


if __name__ == '__main__':
    setup_logging()

    LOOP = asyncio.get_event_loop()
    LOOP.add_signal_handler(signal.SIGINT, exit_gracefully)
    LOOP.add_signal_handler(signal.SIGTERM, exit_gracefully)
    asyncio.ensure_future(run_application(), loop=LOOP)
    LOOP.run_forever()
