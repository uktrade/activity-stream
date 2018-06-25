import asyncio
import datetime
import functools
import hashlib
import hmac
import json
import logging
import os
import re
import signal
import sys

import aiohttp
from aiohttp import web
import mohawk
from mohawk.exc import HawkFail

from .utils import (
    ExpiringSet,
    flatten,
    normalise_environment,
)

EXCEPTION_INTERVAL = 60
NONCE_EXPIRE = 120

NOT_PROVIDED = 'Authentication credentials were not provided.'
INCORRECT = 'Incorrect authentication credentials.'
MISSING_CONTENT_TYPE = 'Content-Type header was not set. ' + \
                       'It must be set for authentication, even if as the empty string.'


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
        'secret_key': key_pair['SECRET_KEY']
    } for key_pair in env['INCOMING_ACCESS_KEY_PAIRS']]
    ip_whitelist = env['INCOMING_IP_WHITELIST']

    es_host = env['ELASTICSEARCH']['HOST']
    es_path = '/_bulk'
    es_endpoint = {
        'host': es_host,
        'path': es_path,
        'access_key_id': env['ELASTICSEARCH']['AWS_ACCESS_KEY_ID'],
        'secret_key': env['ELASTICSEARCH']['AWS_SECRET_ACCESS_KEY'],
        'region': env['ELASTICSEARCH']['REGION'],
        'url': (
            env['ELASTICSEARCH']['PROTOCOL'] + '://' +
            es_host + ':' + env['ELASTICSEARCH']['PORT'] + es_path
        ),
    }

    app_logger.debug('Examining environment: done')

    await create_incoming_application(
        port, ip_whitelist, incoming_key_pairs,
    )
    await create_outgoing_application(
        feed_endpoints, es_endpoint,
    )


async def create_incoming_application(port, ip_whitelist, incoming_key_pairs):
    app_logger = logging.getLogger(__name__)

    async def handle(_):
        return web.json_response({'secret': 'to-be-hidden'})

    app_logger.debug('Creating listening web application...')
    app = web.Application(middlewares=[
        authenticator(ip_whitelist, incoming_key_pairs),
        authorizer(),
    ])
    app.add_routes([
        web.post('/v1/', handle),
        web.get('/v1/', handle),
    ])
    access_log_format = '%a %t "%r" %s %b "%{Referer}i" "%{User-Agent}i" %{X-Forwarded-For}i'

    runner = web.AppRunner(app, access_log_format=access_log_format)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    app_logger.debug('Creating listening web application: done')


def authenticator(ip_whitelist, incoming_key_pairs):
    app_logger = logging.getLogger(__name__)

    def lookup_credentials(passed_access_key_id):
        matching_key_pairs = [
            key_pair
            for key_pair in incoming_key_pairs
            if key_pair['key_id'] == passed_access_key_id
        ]

        if not matching_key_pairs:
            raise HawkFail(f'No Hawk ID of {passed_access_key_id}')

        return {
            'id': matching_key_pairs[0]['key_id'],
            'key': matching_key_pairs[0]['secret_key'],
            'algorithm': 'sha256',
        }

    # This would need to be stored externally if this was ever to be load balanced,
    # otherwise replay attacks could succeed by hitting another instance
    seen_nonces = ExpiringSet(NONCE_EXPIRE)

    def seen_nonce(access_key_id, nonce, _):
        nonce_tuple = (access_key_id, nonce)
        seen = nonce_tuple in seen_nonces
        if not seen:
            seen_nonces.add(nonce_tuple)
        return seen

    async def raise_if_not_authentic(request):
        mohawk.Receiver(
            lookup_credentials,
            request.headers['Authorization'],
            str(request.url),
            request.method,
            content=await request.content.read(),
            content_type=request.headers['Content-Type'],
            seen_nonce=seen_nonce,
        )

    @web.middleware
    async def authenticate(request, handler):
        if 'X-Forwarded-For' not in request.headers:
            app_logger.warning(
                'Failed authentication: no X-Forwarded-For header passed'
            )
            return web.json_response({
                'details': INCORRECT,
            }, status=401)

        remote_address = request.headers['X-Forwarded-For'].split(',')[0].strip()

        if remote_address not in ip_whitelist:
            app_logger.warning(
                'Failed authentication: the X-Forwarded-For header did not '
                'start with an IP in the whitelist'
            )
            return web.json_response({
                'details': INCORRECT,
            }, status=401)

        if 'Authorization' not in request.headers:
            return web.json_response({
                'details': NOT_PROVIDED,
            }, status=401)

        if 'Content-Type' not in request.headers:
            return web.json_response({
                'details': MISSING_CONTENT_TYPE,
            }, status=401)

        try:
            await raise_if_not_authentic(request)
        except HawkFail as exception:
            app_logger.warning('Failed authentication %s', exception)
            return web.json_response({
                'details': INCORRECT,
            }, status=401)

        return await handler(request)

    return authenticate


def authorizer():
    @web.middleware
    async def authorize(request, handler):
        return await handler(request)

    return authorize


async def create_outgoing_application(feed_endpoints, es_endpoint):
    async with aiohttp.ClientSession() as session:
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
        auth_headers = es_bulk_auth_headers(
            access_key=es_endpoint['access_key_id'],
            secret_key=es_endpoint['secret_key'],
            region=es_endpoint['region'],
            host=es_endpoint['host'],
            path=es_endpoint['path'],
            payload=es_bulk_contents,
        )
        es_result = await session.post(
            es_endpoint['url'], data=es_bulk_contents, headers={**headers, **auth_headers})
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


class ElasticsearchBulkFeed():

    polling_page_interval = 0
    polling_seed_interval = 5

    @classmethod
    def parse_config(cls, config):
        return cls(
            seed=config['SEED'],
            access_key_id=config['ACCESS_KEY_ID'],
            secret_access_key=config['SECRET_ACCESS_KEY'],
        )

    def __init__(self, seed, access_key_id, secret_access_key):
        self.seed = seed
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

    @staticmethod
    def next_href(feed):
        return feed['next_url'] if 'next_url' in feed else None

    def auth_headers(self, url):
        method = 'GET'
        return {
            'Authorization': mohawk.Sender({
                'id': self.access_key_id,
                'key': self.secret_access_key,
                'algorithm': 'sha256'
            }, url, method, content_type='', content='').request_header,
        }

    @staticmethod
    def convert_to_bulk_es(feed):
        return feed['items']


class ZendeskFeed():

    # The staging API is severely rate limited
    # This could be dynamic, but KISS
    polling_page_interval = 30
    polling_seed_interval = 60

    company_number_regex = r'Company number:\s*(\d+)'

    @classmethod
    def parse_config(cls, config):
        return cls(
            seed=config['SEED'],
            api_email=config['API_EMAIL'],
            api_key=config['API_KEY'],
        )

    def __init__(self, seed, api_email, api_key):
        self.seed = seed
        self.api_email = api_email
        self.api_key = api_key

    @staticmethod
    def next_href(feed):
        return feed['next_page']

    def auth_headers(self, _):
        return {
            'Authorization': aiohttp.helpers.BasicAuth(
                login=self.api_email + '/token',
                password=self.api_key,
            ).encode()
        }

    @classmethod
    def convert_to_bulk_es(cls, page):
        def company_numbers(description):
            match = re.search(cls.company_number_regex, description)
            return [match[1]] if match else []

        tickets_with_company_numbers = [
            (ticket['id'], ticket['created_at'], company_number)
            for ticket in page['tickets']
            for company_number in company_numbers(ticket['description'])
        ]

        return [{
            'action_and_metadata': {
                'index': {
                    '_index': 'company_timeline',
                    '_type': '_doc',
                    '_id': 'contact-made-' + str(ticket_id),
                },
            },
            'source': {
                'date': created_at,
                'activity': 'contact-made',
                'company_house_number': company_number,
            }
        } for ticket_id, created_at, company_number in tickets_with_company_numbers]


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
