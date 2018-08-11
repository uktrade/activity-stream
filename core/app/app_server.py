import hmac

from aiohttp import web
import mohawk
from mohawk.exc import HawkFail

from shared.utils import (
    random_url_safe,
)

from .app_elasticsearch import (
    es_search,
    es_search_existing_scroll,
    es_search_new_scroll,
)


NOT_PROVIDED = 'Authentication credentials were not provided.'
INCORRECT = 'Incorrect authentication credentials.'
MISSING_CONTENT_TYPE = 'Content-Type header was not set. ' + \
                       'It must be set for authentication, even if as the empty string.'
MISSING_X_FORWARDED_PROTO = 'The X-Forwarded-Proto header was not set.'
NOT_AUTHORIZED = 'You are not authorized to perform this action.'
UNKNOWN_ERROR = 'An unknown error occurred.'


def authenticator(incoming_key_pairs, redis_client, nonce_expire):
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

        try:
            receiver = await _authenticate_or_raise(incoming_key_pairs, redis_client,
                                                    nonce_expire, request)
        except HawkFail as exception:
            request['logger'].warning('Failed authentication %s', exception)
            raise web.HTTPUnauthorized(text=INCORRECT)

        request['permissions'] = receiver.resource.credentials['permissions']
        return await handler(request)

    return authenticate


async def _authenticate_or_raise(incoming_key_pairs, redis_client, nonce_expire, request):
    def lookup_credentials(passed_access_key_id):
        matching_key_pairs = [
            key_pair
            for key_pair in incoming_key_pairs
            if hmac.compare_digest(key_pair['key_id'], passed_access_key_id)
        ]

        if not matching_key_pairs:
            raise HawkFail(f'No Hawk ID of {passed_access_key_id}')

        return {
            'id': matching_key_pairs[0]['key_id'],
            'key': matching_key_pairs[0]['secret_key'],
            'permissions': matching_key_pairs[0]['permissions'],
            'algorithm': 'sha256',
        }

    receiver = mohawk.Receiver(
        lookup_credentials,
        request.headers['Authorization'],
        str(request.url.with_scheme(request.headers['X-Forwarded-Proto'])),
        request.method,
        content=await request.read(),
        content_type=request.headers['Content-Type'],
        # Mohawk doesn't provide an async way of checking nonce
        seen_nonce=lambda _, __, ___: False,
    )

    nonce = receiver.resource.nonce
    access_key_id = receiver.resource.credentials['id']
    nonce_key = f'nonce-{access_key_id}-{nonce}'
    redis_response = await redis_client.execute('SET', nonce_key, '1',
                                                'EX', nonce_expire, 'NX')
    seen_nonce = not redis_response == b'OK'
    if seen_nonce:
        raise web.HTTPUnauthorized(text=INCORRECT)

    return receiver


def authorizer():
    @web.middleware
    async def authorize(request, handler):
        if request.method not in request['permissions']:
            raise web.HTTPForbidden(text=NOT_AUTHORIZED)

        return await handler(request)

    return authorize


def raven_reporter(raven_client):
    @web.middleware
    async def _raven_reporter(request, handler):
        try:
            return await handler(request)
        except (web.HTTPSuccessful, web.HTTPRedirection, web.HTTPClientError):
            raise
        except BaseException:
            raven_client.captureException(data={
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
        except BaseException as exception:
            request['logger'].exception('About to return 500')
            response = json_response({'details': UNKNOWN_ERROR}, status=500)
        return response

    return _convert_errors_to_json


async def handle_post(_):
    return json_response({'secret': 'to-be-hidden'}, status=200)


def handle_get_new(session, redis_client, pagination_expire, es_endpoint):
    return _handle_get(session, redis_client, pagination_expire, es_endpoint,
                       es_search_new_scroll)


def handle_get_existing(session, redis_client, pagination_expire, es_endpoint):
    return _handle_get(session, redis_client, pagination_expire, es_endpoint,
                       es_search_existing_scroll)


def _handle_get(session, redis_client, pagination_expire, es_endpoint, get_path_query):
    async def handle(request):
        incoming_body = await request.read()
        path, query, body = await get_path_query(redis_client, request.match_info,
                                                 incoming_body)

        async def to_public_scroll_url(private_scroll_id):
            public_scroll_id = random_url_safe(8)
            await redis_client.set(f'private-scroll-id-{public_scroll_id}', private_scroll_id,
                                   expire=pagination_expire)
            return str(request.url.join(
                request.app.router['scroll'].url_for(public_scroll_id=public_scroll_id)))

        results, status = await es_search(session, es_endpoint, path, query, body,
                                          {'Content-Type': request.headers['Content-Type']},
                                          to_public_scroll_url)

        return json_response(results, status=status)

    return handle


def handle_get_metrics(redis_client):
    async def handle(_):
        return web.Response(body=await redis_client.get('metrics'), status=200, headers={
            'Content-Type': 'text/plain; charset=utf-8',
        })

    return handle


def json_response(data, status):
    return web.json_response(data, status=status, headers={
        'Server': 'activity-stream'
    })
