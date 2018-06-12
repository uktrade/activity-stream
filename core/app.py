import asyncio
import datetime
import functools
import hashlib
import hmac
import json
import logging
import os
import sys

import aiohttp
from aiohttp import web
import mohawk

POLLING_INTERVAL = 5
EXCEPTION_INTERVAL = 60

NOT_PROVIDED = 'Authentication credentials were not provided.'


async def run_application():
    app_logger = logging.getLogger(__name__)

    app_logger.debug('Examining environment...')
    port = os.environ['PORT']

    feed_endpoints = os.environ['FEED_ENDPOINTS'].split(',')
    feed_auth_header_getter = functools.partial(
        feed_auth_headers,
        access_key=os.environ['FEED_ACCESS_KEY_ID'],
        secret_key=os.environ['FEED_SECRET_ACCESS_KEY'],
    )

    es_host = os.environ['ELASTICSEARCH_HOST']
    es_path = '/_bulk'
    es_bulk_auth_header_getter = functools.partial(
        es_bulk_auth_headers,
        access_key=os.environ['ELASTICSEARCH_AWS_ACCESS_KEY_ID'],
        secret_key=os.environ['ELASTICSEARCH_AWS_SECRET_ACCESS_KEY'],
        region=os.environ['ELASTICSEARCH_REGION'],
        host=es_host,
        path=es_path,
    )
    es_endpoint = os.environ['ELASTICSEARCH_PROTOCOL'] + '://' + \
        es_host + ':' + os.environ['ELASTICSEARCH_PORT'] + es_path
    app_logger.debug('Examining environment: done')

    await create_incoming_application(port)
    await create_outgoing_application(
        feed_auth_header_getter, feed_endpoints,
        es_bulk_auth_header_getter, es_endpoint,
    )


async def create_incoming_application(port):
    app_logger = logging.getLogger(__name__)

    @web.middleware
    async def authenticate(request, handler):
        if 'Authorization' not in request.headers:
            return web.json_response({
                'details': NOT_PROVIDED,
            }, status=401)

        return await handler(request)

    async def handle(_):
        return web.json_response({})

    app_logger.debug('Creating listening web application...')
    app = web.Application(middlewares=[authenticate])
    app.add_routes([web.post('/', handle)])
    access_log_format = '%a %t "%r" %s %b "%{Referer}i" "%{User-Agent}i" %{X-Forwarded-For}i'

    runner = web.AppRunner(app, access_log_format=access_log_format)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    app_logger.debug('Creating listening web application: done')


async def create_outgoing_application(feed_auth_header_getter, feed_endpoints,
                                      es_bulk_auth_header_getter, es_endpoint):
    async with aiohttp.ClientSession() as session:
        feeds = [
            repeat_even_on_exception(functools.partial(
                ingest_feed,
                session, feed_auth_header_getter, feed_endpoint,
                es_bulk_auth_header_getter, es_endpoint
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


async def ingest_feed(session, feed_auth_header_getter, feed_endpoint,
                      es_bulk_auth_header_getter, es_endpoint):
    app_logger = logging.getLogger(__name__)

    async for feed in poll(session, feed_auth_header_getter, feed_endpoint):
        app_logger.debug('Converting feed to ES bulk ingest commands...')
        es_bulk_contents = es_bulk(feed).encode('utf-8')
        app_logger.debug('Converting to ES bulk ingest commands: done (%s)', es_bulk_contents)

        app_logger.debug('POSTing bulk import to ES...')
        headers = {
            'Content-Type': 'application/x-ndjson'
        }
        auth_headers = es_bulk_auth_header_getter(payload=es_bulk_contents)
        es_result = await session.post(
            es_endpoint, data=es_bulk_contents, headers={**headers, **auth_headers})
        app_logger.debug('Pushing to ES: done (%s)', await es_result.content.read())


async def poll(session, feed_auth_header_getter, seed_url):
    app_logger = logging.getLogger(__name__)

    href = seed_url
    while True:
        app_logger.debug('Polling')
        result = await session.get(href, headers=feed_auth_header_getter(url=href))

        app_logger.debug('Fetching contents of feed...')
        feed_contents = await result.content.read()
        app_logger.debug('Fetching contents of feed: done (%s)', feed_contents)

        app_logger.debug('Parsing JSON...')
        feed = json.loads(feed_contents)
        app_logger.debug('Parsed')

        yield feed

        app_logger.debug('Finding next URL...')
        href = next_href(feed)
        app_logger.debug('Finding next URL: done (%s)', href)

        if href:
            app_logger.debug('Will immediatly poll (%s)', href)
        else:
            href = seed_url
            app_logger.debug('Going back to seed')
            app_logger.debug('Waiting to poll (%s)', href)
            await asyncio.sleep(POLLING_INTERVAL)


def next_href(feed):
    return feed['next_url'] if 'next_url' in feed else None


def es_bulk(feed):
    return '\n'.join(flatten([
        [json.dumps(item['action_and_metadata'], sort_keys=True),
         json.dumps(item['source'], sort_keys=True)]
        for item in feed['items']
    ])) + '\n'


def feed_auth_headers(access_key, secret_key, url):
    method = 'GET'
    return {
        'Authorization': mohawk.Sender({
            'id': access_key,
            'key': secret_key,
            'algorithm': 'sha256'
        }, url, method, content_type='', content='').request_header,
    }


def es_bulk_auth_headers(access_key, secret_key, region, host, path, payload):
    service = 'es'
    method = 'POST'
    signed_headers = 'content-type;host;x-amz-date'
    algorithm = 'AWS4-HMAC-SHA256'

    now = datetime.datetime.utcnow()
    amzdate = now.strftime('%Y%m%dT%H%M%SZ')
    datestamp = now.strftime('%Y%m%d')

    credential_scope = f'{datestamp}/{region}/{service}/aws4_request'

    def signature():
        def canonical_request():
            canonical_uri = path
            canonical_querystring = ''
            canonical_headers = \
                f'content-type:application/x-ndjson\n' + \
                f'host:{host}\nx-amz-date:{amzdate}\n'
            payload_hash = hashlib.sha256(payload).hexdigest()

            return f'{method}\n{canonical_uri}\n{canonical_querystring}\n' + \
                   f'{canonical_headers}\n{signed_headers}\n{payload_hash}'

        def sign(key, msg):
            return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

        string_to_sign = \
            f'{algorithm}\n{amzdate}\n{credential_scope}\n' + \
            hashlib.sha256(canonical_request().encode('utf-8')).hexdigest()

        date_key = sign(('AWS4' + secret_key).encode('utf-8'), datestamp)
        region_key = sign(date_key, region)
        service_key = sign(region_key, service)
        request_key = sign(service_key, 'aws4_request')
        return sign(request_key, string_to_sign).hex()

    return {
        'x-amz-date': amzdate,
        'Authorization': (
            f'{algorithm} Credential={access_key}/{credential_scope}, ' +
            f'SignedHeaders={signed_headers}, Signature=' + signature()
        ),
    }


def flatten(list_to_flatten):
    return [
        item
        for sublist in list_to_flatten
        for item in sublist
    ]


def setup_logging():
    stdout_handler = logging.StreamHandler(sys.stdout)
    aiohttp_log = logging.getLogger('aiohttp.access')
    aiohttp_log.setLevel(logging.DEBUG)
    aiohttp_log.addHandler(stdout_handler)

    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(logging.DEBUG)
    app_logger.addHandler(stdout_handler)


if __name__ == '__main__':
    setup_logging()

    LOOP = asyncio.get_event_loop()
    asyncio.ensure_future(run_application(), loop=LOOP)
    LOOP.run_forever()
