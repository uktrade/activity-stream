import hmac
import time

from ipaddress import IPv4Network, IPv4Address

from aiohttp import web

from .app_incoming_elasticsearch import (
    es_search_filtered,
)
from .elasticsearch import (
    es_request,
    # es_min_verification_age,
)
from .app_incoming_hawk import (
    authenticate_hawk_header,
)
from .logger import (
    logged,
    get_child_logger,
)
from .utils import (
    get_child_context,
    json_loads,
    json_dumps,
    random_url_safe,
)
from .app_incoming_redis import (
    redis_get_metrics,
    get_feeds_status,
)

NOT_PROVIDED = 'Authentication credentials were not provided.'
INCORRECT = 'Incorrect authentication credentials.'
MISSING_CONTENT_TYPE = 'Content-Type header was not set. ' + \
                       'It must be set for authentication, even if as the empty string.'
MISSING_X_FORWARDED_PROTO = 'The X-Forwarded-Proto header was not set.'
UNKNOWN_ERROR = 'An unknown error occurred.'


def authenticator(context, incoming_key_pairs, nonce_expire):

    def _lookup_credentials(passed_access_key_id):
        return lookup_credentials(incoming_key_pairs, passed_access_key_id)

    @web.middleware
    async def authenticate(request, handler):
        if 'X-Forwarded-Proto' not in request.headers:
            request['logger'].warning(
                'Failed authentication: no X-Forwarded-Proto header passed'
            )
            raise web.HTTPUnauthorized(text=MISSING_X_FORWARDED_PROTO)

        if 'Authorization' not in request.headers:
            raise web.HTTPUnauthorized(text=NOT_PROVIDED)

        if 'Content-Type' not in request.headers:
            raise web.HTTPUnauthorized(text=MISSING_CONTENT_TYPE)

        is_authentic, private_error_message, credentials = await authenticate_hawk_header(
            context=context,
            nonce_expire=nonce_expire,
            lookup_credentials=_lookup_credentials,
            header=request.headers['Authorization'],
            method=request.method,
            host=request.url.host,
            port=str(request.url.with_scheme(request.headers['X-Forwarded-Proto']).port),
            path=request.url.raw_path_qs,
            content_type=request.headers['Content-Type'].encode('utf-8'),
            content=await request.read()
        )

        if not is_authentic:
            request['logger'].warning('Failed authentication (%s)', private_error_message)
            raise web.HTTPUnauthorized(text=INCORRECT)

        request['logger'] = get_child_logger(
            request['logger'],
            credentials['id'],
        )
        request['permissions'] = credentials['permissions']
        return await handler(request)

    return authenticate


def lookup_credentials(incoming_key_pairs, passed_access_key_id):
    matching_key_pairs = [
        key_pair
        for key_pair in incoming_key_pairs
        if hmac.compare_digest(key_pair['key_id'], passed_access_key_id)
    ]

    return {
        'id': matching_key_pairs[0]['key_id'],
        'key': matching_key_pairs[0]['secret_key'],
        'permissions': matching_key_pairs[0]['permissions'],
    } if matching_key_pairs else None


def raven_reporter(context):
    @web.middleware
    async def _raven_reporter(request, handler):
        try:
            return await handler(request)
        except (web.HTTPSuccessful, web.HTTPRedirection, web.HTTPClientError):
            raise
        except BaseException:
            context.raven_client.captureException(data={
                'request': {
                    'url': str(request.url.with_scheme(request.headers['X-Forwarded-Proto'])),
                    'query_string': request.query_string,
                    'method': request.method,
                    'data': await request.read(),
                    'headers':  dict(request.headers),
                }
            })
            raise

    return _raven_reporter


def convert_errors_to_json():
    @web.middleware
    async def _convert_errors_to_json(request, handler):
        try:
            response = await handler(request)
        except web.HTTPException as exception:
            response = json_response({'details': exception.text}, status=exception.status_code)
        except BaseException:
            request['logger'].exception('About to return 500')
            response = json_response({'details': UNKNOWN_ERROR}, status=500)
        return response

    return _convert_errors_to_json


def handle_get_p1_check(parent_context):

    async def handle(_):
        context = get_child_context(parent_context, 'check')

        with logged(context.logger.debug, context.logger.warning, 'Checking', []):
            await context.redis_client.execute('SET', 'redis-check', b'GREEN', 'EX', 1)
            redis_result = await context.redis_client.execute('GET', 'redis-check')
            is_redis_green = redis_result == b'GREEN'

            # Ideally we would check ES, but it seems to now return 429s even when not hitting it
            # that often and everything else is fine, it's just the healthcheck that fails. We
            # have some indirect checks on the Activity Stream (e.g. Great Search), so we have
            # disabled the check on Elasticsearch. Leaving the code commented here so it's
            # more clear where happened if there are other incidents.

            # min_age = await es_min_verification_age(context)
            # is_elasticsearch_green = min_age < 60 * 60 * 12

            # all_green = is_redis_green and is_elasticsearch_green

            # status = \
            #     (b'__UP__' if all_green else b'__DOWN__') + b'\n' + \
            #     (b'redis:' + (b'GREEN' if is_redis_green else b'RED')) + b'\n' + \
            #     (b'elasticsearch:' + (b'GREEN' if is_elasticsearch_green else b'RED')) + b'\n'

            status = \
                (b'__UP__' if is_redis_green else b'__DOWN__') + b'\n'

        return web.Response(body=status, status=200, headers={
            'Content-Type': 'text/plain; charset=utf-8',
        })

    return handle


def handle_get_p2_check(parent_context, feeds):
    start_counter = time.perf_counter()

    # Grace period after uptime to allow new feeds to start reporting
    # without making the service appear down
    grace = 30

    async def handle(_):
        context = get_child_context(parent_context, 'check')

        with logged(context.logger.debug, context.logger.warning, 'Checking', []):
            uptime = time.perf_counter() - start_counter
            in_grace_period = uptime <= grace

            # The status of the feeds are via Redis...
            # - To actually reflect if each was recently sucessful, since it is done by the
            #   outgoing application, not this one
            # - To keep the guarantee that we only make a single request to each feed at any one
            #   time (locking between the outoing application and this one would be tricky)
            feeds_statuses = await get_feeds_status(context, [
                feed.unique_id for feed in feeds
            ])
            feeds_status_green_if_grace = [
                b'RED' if uptime > feed.down_grace_period + grace and status != b'GREEN' else
                b'GREEN'
                for feed, status in zip(feeds, feeds_statuses)
            ]
            all_green = all([status == b'GREEN' for status in feeds_status_green_if_grace])

            status = \
                (b'__UP__' if all_green else b'__DOWN__') + \
                (b' (IN_STARTUP_GRACE_PERIOD)' if in_grace_period else b'') + b'\n' + \
                b''.join([
                    feed.unique_id.encode('utf-8') + b':' + feeds_status_green_if_grace[i] + b'\n'
                    for (i, feed) in enumerate(feeds)
                ])

        return web.Response(body=status, status=200, headers={
            'Content-Type': 'text/plain; charset=utf-8',
        })

    return handle


def handle_get_metrics(context):
    async def handle(_):
        return web.Response(body=await redis_get_metrics(context), status=200, headers={
            'Content-Type': 'text/plain; charset=utf-8',
        })

    return handle


def handle_root(context):

    async def handle(_):
        results = await es_request(
            context=context,
            method='GET',
            path='/',
            query={},
            headers={},
            payload=b''
        )

        return web.Response(body=results._body, status=results.status, headers={
            'Content-Type': 'application/json; charset=utf-8',
            'Server': 'activity-stream'
        })

    return handle


def handle_get_search_v2(context, alias):

    async def handle(request):
        results = await es_request(
            context=context,
            method='GET',
            path=f'/{alias}/_search',
            query={},
            headers={'Content-Type': request.headers['Content-Type']},
            payload=json_dumps(
                es_search_filtered(
                    request['permissions'][alias], json_loads(await request.read())
                )
            ),
        )

        return web.Response(body=results._body, status=results.status, headers={
            'Content-Type': 'application/json; charset=utf-8',
            'Server': 'activity-stream'
        })

    return handle


def json_response(data, status):
    return web.json_response(data, status=status, headers={
        'Server': 'activity-stream'
    })


def server_logger(logger):

    @web.middleware
    async def _server_logger(request, handler):
        child_logger = get_child_logger(logger, random_url_safe(8))
        request['logger'] = child_logger
        child_logger.info('Receiving request (%s) (%s %s HTTP/%s.%s) (%s) (%s)', *(
            (
                request.remote,
                request.method,
                request.path_qs,
            ) +
            request.version +
            (
                request.headers.get('User-Agent', '-'),
                request.headers.get('X-Forwarded-For', '-'),
            )
        ))

        with logged(child_logger.info, child_logger.error, 'Processing request', []):
            response = await handler(request)

        child_logger.debug(
            'Sending Response (%s) (%s)',
            response.status, response.content_length,
        )

        return response

    return _server_logger


def authenticate_by_ip(incorrect, ip_whitelist):

    @web.middleware
    async def _authenticate_by_ip(request, handler):
        if 'X-Forwarded-For' not in request.headers:
            request['logger'].warning(
                'Failed authentication: no X-Forwarded-For header passed'
            )
            raise web.HTTPUnauthorized(text=incorrect)

        # PaaS appends 2 IPs, where the IP connected from is the first of the two
        ip_addesses = request.headers['X-Forwarded-For'].split(',')
        if len(ip_addesses) < 2:
            request['logger'].warning(
                'Failed authentication: the X-Forwarded-For header does not '
                'contain enough IP addresses'
            )
            raise web.HTTPUnauthorized(text=incorrect)

        remote_address = ip_addesses[-2].strip()

        is_allowed = any(
            IPv4Address(remote_address) in IPv4Network(address_or_subnet)
            for address_or_subnet in ip_whitelist
        )
        if not is_allowed:
            request['logger'].warning(
                'Failed authentication: the IP address derived from the '
                'X-Forwarded-For header is not in the whitelist'
            )
            raise web.HTTPUnauthorized(text=incorrect)

        return await handler(request)

    return _authenticate_by_ip
