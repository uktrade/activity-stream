import datetime
import hashlib
import hmac
import json
import logging

from aiohttp.web import (
    HTTPNotFound,
)

from .app_utils import (
    flatten,
)


async def ensure_index(session, es_endpoint):
    index_definition = json.dumps({}).encode('utf-8')
    path = '/activities'
    auth_headers = es_auth_headers(
        endpoint=es_endpoint,
        method='PUT',
        path=path,
        query_string='',
        content_type='application/json',
        payload=index_definition,
    )
    headers = {
        'Content-Type': 'application/json',
    }
    url = es_endpoint['base_url'] + path
    results = await session.put(
        url, data=index_definition, headers={**headers, **auth_headers})
    data = await results.json()
    index_exists = (
        results.status == 400 and data['error']['type'] == 'resource_already_exists_exception'
    )
    if (results.status != 200 and not index_exists):
        raise Exception(await results.text())


async def ensure_mappings(session, es_endpoint):
    mapping_definition = json.dumps({
        'properties': {
            'published_date': {
                'type': 'date',
            },
            'type': {
                'type': 'keyword',
            },
            'object.type': {
                'type': 'keyword',
            },
        },
    }).encode('utf-8')
    path = '/activities/_mapping/_doc'
    auth_headers = es_auth_headers(
        endpoint=es_endpoint,
        method='PUT',
        path=path,
        query_string='',
        content_type='application/json',
        payload=mapping_definition,
    )
    headers = {
        'Content-Type': 'application/json',
    }
    url = es_endpoint['base_url'] + path
    results = await session.put(
        url, data=mapping_definition, headers={**headers, **auth_headers})
    if results.status != 200:
        raise Exception(await results.text())


def es_auth_headers(endpoint, method, path, query_string, content_type, payload):
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
            canonical_querystring = query_string
            canonical_headers = \
                f'content-type:{content_type}\n' + \
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


def es_search_new_scroll(_, __, query):
    return '/activities/_search', 'scroll=30s', query


def es_search_existing_scroll(public_to_private_scroll_ids, match_info, _):
    # This is not wrapped in a try/except. This function should only be
    # called if public_scroll_id is in match_info, and there is some server
    # error if this isn't present, and so bubbling up and resulting in a 500
    # is appropriate if a KeyError is thrown
    public_scroll_id = match_info['public_scroll_id']

    try:
        private_scroll_id = public_to_private_scroll_ids[public_scroll_id]
    except KeyError:
        # It can be argued that this function shouldn't have knowledge that
        # it's called from a HTTP request. However, that _is_ its only use,
        # so KISS, and not introduce more layers unless needed
        raise HTTPNotFound(text='Scroll ID not found.')

    return '/_search/scroll', 'scroll=30s', json.dumps({
        'scroll_id': private_scroll_id,
    }).encode('utf-8')


async def es_search(session, es_endpoint, path, query_string, body,
                    content_type, to_public_scroll_url):
    url = es_endpoint['base_url'] + path + (('?' + query_string) if query_string != '' else '')

    auth_headers = es_auth_headers(
        endpoint=es_endpoint,
        method='GET',
        path=path,
        query_string=query_string,
        content_type=content_type,
        payload=body,
    )

    results = await session.get(
        url,
        headers={**auth_headers, **{
            'Content-Type': content_type,
        }},
        data=body,
    )

    response = await results.json()
    return \
        (activities(response, to_public_scroll_url), 200) if results.status == 200 else \
        (response, results.status)


def activities(elasticsearch_reponse, to_public_scroll_url):
    elasticsearch_hits = elasticsearch_reponse['hits'].get('hits', [])
    private_scroll_id = elasticsearch_reponse['_scroll_id']
    next_dict = {
        'next': to_public_scroll_url(private_scroll_id)
    } if elasticsearch_hits else {}

    return {**{
        '@context': [
            'https://www.w3.org/ns/activitystreams',
            {
                'dit': 'https://www.trade.gov.uk/ns/activitystreams/v1',
            }
        ],
        'orderedItems': [
            item['_source']
            for item in elasticsearch_hits
        ],
        'type': 'Collection',
    }, **next_dict}


async def es_bulk(session, es_endpoint, items):
    app_logger = logging.getLogger(__name__)

    app_logger.debug('Converting feed to ES bulk ingest commands...')
    es_bulk_contents = ('\n'.join(flatten([
        [json.dumps(item['action_and_metadata'], sort_keys=True),
         json.dumps(item['source'], sort_keys=True)]
        for item in items
    ])) + '\n').encode('utf-8')
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
        query_string='',
        content_type='application/x-ndjson',
        payload=es_bulk_contents,
    )
    url = es_endpoint['base_url'] + path
    es_result = await session.post(
        url, data=es_bulk_contents, headers={**headers, **auth_headers})

    app_logger.debug('Pushing to ES: done (%s)', await es_result.content.read())
