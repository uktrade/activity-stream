import asyncio
import datetime
import functools
import hashlib
import hmac
import json
import logging
import os
import signal
import sys

import aiohttp
from aiohttp import web

from .app_feeds import (
    ElasticsearchBulkFeed,
    ZendeskFeed,
)
from .app_server import (
    authenticator,
    authorizer,
    handle_get,
    handle_post,
)
from .app_utils import (
    flatten,
    normalise_environment,
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
            'elasticsearch_bulk': ElasticsearchBulkFeed,
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
        await create_incoming_application(
            port, ip_whitelist, incoming_key_pairs,
        )
        await create_outgoing_application(
            session, feed_endpoints, es_endpoint,
        )


async def create_incoming_application(port, ip_whitelist, incoming_key_pairs):
    app_logger = logging.getLogger(__name__)

    app_logger.debug('Creating listening web application...')
    app = web.Application(middlewares=[
        authenticator(ip_whitelist, incoming_key_pairs, NONCE_EXPIRE),
        authorizer(),
    ])
    app.add_routes([
        web.post('/v1/', handle_post),
        web.get('/v1/', handle_get()),
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
        ))
        for feed_endpoint in feed_endpoints
    ]
    await asyncio.gather(*feeds)


async def repeat_even_on_exception(never_ending_coroutine):
    app_logger = logging.getLogger(__name__)

    while True:
        try:
            await never_ending_coroutine()
        except BaseException as exception:
            app_logger.warning('Polling feed raised exception: %s', exception)
        else:
            app_logger.warning(
                'Polling feed finished without exception. '
                'This is not expected: it should run forever.'
            )
        finally:
            app_logger.warning('Waiting %s seconds until restarting the feed', EXCEPTION_INTERVAL)
            await asyncio.sleep(EXCEPTION_INTERVAL)


async def ingest_feed(session, feed_endpoint, es_endpoint):
    app_logger = logging.getLogger(__name__)

    async for feed in poll(session, feed_endpoint):
        app_logger.debug('Converting feed to ES bulk ingest commands...')
        es_bulk_contents = es_bulk(feed).encode('utf-8')
        app_logger.debug('Converting to ES bulk ingest commands: done (%s)', es_bulk_contents)

        app_logger.debug('POSTing bulk import to ES...')
        headers = {
            'Content-Type': 'application/x-ndjson'
        }
        path = '/_bulk'
        auth_headers = es_auth_headers(
            endpoint=es_endpoint,
            method='POST',
            path='/_bulk',
            payload=es_bulk_contents,
        )
        url = es_endpoint['base_url'] + path
        es_result = await session.post(
            url, data=es_bulk_contents, headers={**headers, **auth_headers})
        app_logger.debug('Pushing to ES: done (%s)', await es_result.content.read())


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


def es_bulk(items):
    return '\n'.join(flatten([
        [json.dumps(item['action_and_metadata'], sort_keys=True),
         json.dumps(item['source'], sort_keys=True)]
        for item in items
    ])) + '\n'


def es_auth_headers(endpoint, method, path, payload):
    service = 'es'
    signed_headers = 'content-type;host;x-amz-date'
    algorithm = 'AWS4-HMAC-SHA256'

    now = datetime.datetime.utcnow()
    amzdate = now.strftime('%Y%m%dT%H%M%SZ')
    datestamp = now.strftime('%Y%m%d')

    credential_scope = f'{datestamp}/{endpoint["region"]}/{service}/aws4_request'

    def signature():
        def canonical_request():
            canonical_uri = path
            canonical_querystring = ''
            canonical_headers = \
                f'content-type:application/x-ndjson\n' + \
                f'host:{endpoint["host"]}\nx-amz-date:{amzdate}\n'
            payload_hash = hashlib.sha256(payload).hexdigest()

            return f'{method}\n{canonical_uri}\n{canonical_querystring}\n' + \
                   f'{canonical_headers}\n{signed_headers}\n{payload_hash}'

        def sign(key, msg):
            return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

        string_to_sign = \
            f'{algorithm}\n{amzdate}\n{credential_scope}\n' + \
            hashlib.sha256(canonical_request().encode('utf-8')).hexdigest()

        date_key = sign(('AWS4' + endpoint['secret_key']).encode('utf-8'), datestamp)
        region_key = sign(date_key, endpoint['region'])
        service_key = sign(region_key, service)
        request_key = sign(service_key, 'aws4_request')
        return sign(request_key, string_to_sign).hex()

    return {
        'x-amz-date': amzdate,
        'Authorization': (
            f'{algorithm} Credential={endpoint["access_key_id"]}/{credential_scope}, ' +
            f'SignedHeaders={signed_headers}, Signature=' + signature()
        ),
    }


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
