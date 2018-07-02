import hmac
import logging

import aiohttp
from aiohttp import web
import mohawk
from mohawk.exc import HawkFail

from .app_elasticsearch import (
    es_search,
    es_search_existing_scroll,
    es_search_new_scroll,
)
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
            content=await request.read(),
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


def handle_get_new(session, es_endpoint):
    return _handle_get(session, es_endpoint, es_search_new_scroll)


def handle_get_existing(session, es_endpoint):
    return _handle_get(session, es_endpoint, es_search_existing_scroll)


def _handle_get(session, es_endpoint, get_path):
    app_logger = logging.getLogger(__name__)

    async def handle(request):
        incoming_body = await request.read()
        path = get_path(request.match_info)

        def get_scroll_url(scroll_id):
            return str(request.url.join(request.app.router['scroll'].url_for(scroll_id=scroll_id)))

        succesful_http = False
        try:
            results, status = await es_search(session, es_endpoint, path, incoming_body,
                                              request.headers['Content-Type'],
                                              get_scroll_url)
            succesful_http = True
        except aiohttp.ClientError as exception:
            app_logger.warning('Error connecting to Elasticsearch: %s', exception)

        return \
            json_response(results, status=status) if succesful_http else \
            json_response({'details': 'An unknown error occurred.'}, status=500)

    return handle


def json_response(data, status):
    return web.json_response(data, status=status, headers={
        'Server': 'activity-stream'
    })
