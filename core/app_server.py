import logging

from aiohttp import web
import mohawk
from mohawk.exc import HawkFail

from .app_utils import (
    ExpiringSet,
)


NOT_PROVIDED = 'Authentication credentials were not provided.'
INCORRECT = 'Incorrect authentication credentials.'
MISSING_CONTENT_TYPE = 'Content-Type header was not set. ' + \
                       'It must be set for authentication, even if as the empty string.'
NOT_AUTHORIZED = 'You are not authorized to perform this action.'


def authenticator(ip_whitelist, incoming_key_pairs, nonce_expire):
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
            'permissions': matching_key_pairs[0]['permissions'],
            'algorithm': 'sha256',
        }

    # This would need to be stored externally if this was ever to be load balanced,
    # otherwise replay attacks could succeed by hitting another instance
    seen_nonces = ExpiringSet(nonce_expire)

    def seen_nonce(access_key_id, nonce, _):
        nonce_tuple = (access_key_id, nonce)
        seen = nonce_tuple in seen_nonces
        if not seen:
            seen_nonces.add(nonce_tuple)
        return seen

    async def authenticate_or_raise(request):
        return mohawk.Receiver(
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
            return json_response({
                'details': INCORRECT,
            }, status=401)

        remote_address = request.headers['X-Forwarded-For'].split(',')[0].strip()

        if remote_address not in ip_whitelist:
            app_logger.warning(
                'Failed authentication: the X-Forwarded-For header did not '
                'start with an IP in the whitelist'
            )
            return json_response({
                'details': INCORRECT,
            }, status=401)

        if 'Authorization' not in request.headers:
            return json_response({
                'details': NOT_PROVIDED,
            }, status=401)

        if 'Content-Type' not in request.headers:
            return json_response({
                'details': MISSING_CONTENT_TYPE,
            }, status=401)

        try:
            receiver = await authenticate_or_raise(request)
        except HawkFail as exception:
            app_logger.warning('Failed authentication %s', exception)
            return json_response({
                'details': INCORRECT,
            }, status=401)

        request['permissions'] = receiver.resource.credentials['permissions']
        return await handler(request)

    return authenticate


def authorizer():
    @web.middleware
    async def authorize(request, handler):
        if request.method not in request['permissions']:
            return json_response({
                'details': NOT_AUTHORIZED,
            }, status=403)
        return await handler(request)

    return authorize


async def handle_post(_):
    return json_response({'secret': 'to-be-hidden'}, status=200)


def handle_get(session, es_auth_headers, es_endpoint):
    path = '/_search'
    url = es_endpoint['base_url'] + path

    async def handle(_):
        auth_headers = es_auth_headers(
            endpoint=es_endpoint,
            method='GET',
            path=path,
            payload=b'',
        )

        results = await session.get(url, headers=auth_headers)
        return json_response(await results.json(), status=200)

    return handle


def json_response(data, status):
    return web.json_response(data, status=status, headers={
        'Server': 'activity-stream'
    })
