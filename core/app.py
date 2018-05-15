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
from lxml import etree

POLLING_INTERVAL = 5


async def run_application():
    app_logger = logging.getLogger(__name__)

    app_logger.debug('Examining environment...')
    PORT = os.environ['PORT']
    FEED_ENDPOINT = os.environ['FEED_ENDPOINT']
    SHARED_SECRET = os.environ['INTERNAL_API_SHARED_SECRET']
    feed_params = {'shared_secret': SHARED_SECRET}

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

    async def handle(request):
        return web.Response(text='')

    app_logger.debug('Creating listening web application...')
    app = web.Application()
    app.add_routes([web.get('/', handle)])
    access_log_format = '%a %t "%r" %s %b "%{Referer}i" "%{User-Agent}i" %{X-Forwarded-For}i'

    runner = web.AppRunner(app, access_log_format=access_log_format)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    app_logger.debug('Creating listening web application: done')

    async with aiohttp.ClientSession() as session:
        async for feed in poll(app_logger, session, feed_params, FEED_ENDPOINT):
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


async def poll(app_logger, session, feed_params, seed_url):
    href = seed_url
    while True:
        app_logger.debug('Polling')
        result = await session.get(href, params=feed_params)

        app_logger.debug('Fetching contents of feed...')
        feed_contents = await result.content.read()
        app_logger.debug('Fetching contents of feed: done (%s)', feed_contents)

        app_logger.debug('Parsing XML...')
        feed = etree.XML(feed_contents)
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
    next_link = feed.xpath('atom:link[@rel="next"]', namespaces={
                           'atom': 'http://www.w3.org/2005/Atom'})
    return next_link[0].attrib['href'] if len(next_link) else None


def es_bulk(feed):
    return '\n'.join(flatten([
        [json.dumps(contents['action_and_metadata']), json.dumps(contents['source'])]
        for es_bulk in feed.iter('{http://trade.gov.uk/activity-stream/v1}elastic_search_bulk')
        for contents in [json.loads(es_bulk.text)]
    ])) + '\n'


def es_bulk_auth_headers(access_key, secret_key, region, host, path, payload):
    service = 'es'
    method = 'POST'

    def sign(key, msg):
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

    def getSignatureKey(key, dateStamp, regionName, serviceName):
        kDate = sign(('AWS4' + key).encode('utf-8'), dateStamp)
        kRegion = sign(kDate, regionName)
        kService = sign(kRegion, serviceName)
        kSigning = sign(kService, 'aws4_request')
        return kSigning

    t = datetime.datetime.utcnow()
    amzdate = t.strftime('%Y%m%dT%H%M%SZ')
    datestamp = t.strftime('%Y%m%d')
    canonical_uri = path
    canonical_querystring = ''
    canonical_headers = 'content-type:application/x-ndjson\n' + \
        'host:' + host + '\n' + 'x-amz-date:' + amzdate + '\n'
    signed_headers = 'content-type;host;x-amz-date'
    payload_hash = hashlib.sha256(payload).hexdigest()

    canonical_request = method + '\n' + canonical_uri + '\n' + canonical_querystring + \
        '\n' + canonical_headers + '\n' + signed_headers + '\n' + payload_hash

    algorithm = 'AWS4-HMAC-SHA256'
    credential_scope = datestamp + '/' + region + '/' + service + '/' + 'aws4_request'
    string_to_sign = algorithm + '\n' + amzdate + '\n' + credential_scope + \
        '\n' + hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
    signing_key = getSignatureKey(secret_key, datestamp, region, service)
    signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()
    authorization_header = (algorithm + ' ' + 'Credential=' + access_key + '/' + credential_scope +
                            ', ' + 'SignedHeaders=' + signed_headers +
                            ', ' + 'Signature=' + signature)

    return {
        'x-amz-date': amzdate,
        'Authorization': authorization_header,
    }


def flatten(l):
    return [item for sublist in l for item in sublist]


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

    loop = asyncio.get_event_loop()
    asyncio.ensure_future(run_application(), loop=loop)
    loop.run_forever()
