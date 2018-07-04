import hmac
import logging
import uuid

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
MISSING_X_FORWARDED_PROTO = 'The X-Forwarded-Proto header was not set.'
NOT_AUTHORIZED = 'You are not authorized to perform this action.'
UNKNOWN_ERROR = 'An unknown error occurred.'


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
            str(request.url.with_scheme(request.headers['X-Forwarded-Proto'])),
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
            raise web.HTTPUnauthorized(text=INCORRECT)

        remote_address = request.headers['X-Forwarded-For'].split(',')[0].strip()

        if remote_address not in ip_whitelist:
            app_logger.warning(
                'Failed authentication: the X-Forwarded-For header did not '
                'start with an IP in the whitelist'
            )
            raise web.HTTPUnauthorized(text=INCORRECT)

        if 'X-Forwarded-Proto' not in request.headers:
            app_logger.warning(
                'Failed authentication: no X-Forwarded-Proto header passed'
            )
            raise web.HTTPUnauthorized(text=MISSING_X_FORWARDED_PROTO)

        if 'Authorization' not in request.headers:
            raise web.HTTPUnauthorized(text=NOT_PROVIDED)

        if 'Content-Type' not in request.headers:
            raise web.HTTPUnauthorized(text=MISSING_CONTENT_TYPE)

        try:
            receiver = await authenticate_or_raise(request)
        except HawkFail as exception:
            app_logger.warning('Failed authentication %s', exception)
            raise web.HTTPUnauthorized(text=INCORRECT)

        request['permissions'] = receiver.resource.credentials['permissions']
        return await handler(request)

    return authenticate


def authorizer():
    @web.middleware
    async def authorize(request, handler):
        if request.method not in request['permissions']:
            raise web.HTTPForbidden(text=NOT_AUTHORIZED)

        return await handler(request)

    return authorize


def convert_errors_to_json():
    app_logger = logging.getLogger(__name__)

    @web.middleware
    async def _convert_errors_to_json(request, handler):
        try:
            response = await handler(request)
        except web.HTTPException as exception:
            response = json_response({'details': exception.text}, status=exception.status_code)
        except BaseException as exception:
            app_logger.warning('Exception: %s', exception)
            response = json_response({'details': UNKNOWN_ERROR}, status=500)
        return response

    return _convert_errors_to_json


async def handle_post(_):
    return json_response({'secret': 'to-be-hidden'}, status=200)


def handle_get_new(session, public_to_private_scroll_ids, es_endpoint):
    return _handle_get(session, public_to_private_scroll_ids,
                       es_endpoint, es_search_new_scroll)


def handle_get_existing(session, public_to_private_scroll_ids, es_endpoint):
    return _handle_get(session, public_to_private_scroll_ids,
                       es_endpoint, es_search_existing_scroll)


def _handle_get(session, public_to_private_scroll_ids, es_endpoint, get_path_query):
    app_logger = logging.getLogger(__name__)

    async def handle(request):
        incoming_body = await request.read()
        path, query_string, body = get_path_query(public_to_private_scroll_ids,
                                                  request.match_info, incoming_body)

        def to_public_scroll_url(private_scroll_id):
            public_scroll_id = uuid.uuid4().hex
            public_to_private_scroll_ids[public_scroll_id] = private_scroll_id
            return str(request.url.join(
                request.app.router['scroll'].url_for(public_scroll_id=public_scroll_id)))

        succesful_http = False
        try:
            results, status = await es_search(session, es_endpoint, path, query_string, body,
                                              request.headers['Content-Type'],
                                              to_public_scroll_url)
            succesful_http = True
        except aiohttp.ClientError as exception:
            app_logger.warning('Error connecting to Elasticsearch: %s', exception)

        return \
            json_response(results, status=status) if succesful_http else \
            json_response({'details': UNKNOWN_ERROR}, status=500)

    return handle


def json_response(data, status):
    return web.json_response(data, status=status, headers={
        'Server': 'activity-stream'
    })
