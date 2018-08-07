import datetime
import hashlib
import hmac
import json
import logging
import os
import time
import urllib.parse

from aiohttp.web import (
    HTTPNotFound,
)

from .app_metrics import (
    async_counter,
    async_timer,
)
from .app_utils import (
    flatten,
)


ALIAS = 'activities'


def get_new_index_name(feed_unique_id):
    today = datetime.date.today().isoformat()
    now = str(datetime.datetime.now().timestamp()).split('.')[0]
    unique = ''.join(os.urandom(5).hex())

    # Storing metadata in index name allows operations to match on
    # them, both by elasticsearch itself, and by regex in Python
    return '' \
        f'{ALIAS}__' \
        f'feed_id_{feed_unique_id}__' \
        f'date_{today}__' \
        f'timestamp_{now}__' \
        f'batch_id_{unique}__'


def indexes_matching_feeds(index_names, feed_unique_ids):
    return flatten([
        indexes_matching_feed(index_names, feed_unique_id)
        for feed_unique_id in feed_unique_ids
    ])


def indexes_matching_feed(index_names, feed_unique_id):
    return [
        index_name
        for index_name in index_names
        if f'{ALIAS}__feed_id_{feed_unique_id}__' in index_name
    ]


def indexes_matching_no_feeds(index_names, feed_unique_ids):
    indexes_matching = indexes_matching_feeds(index_names, feed_unique_ids)
    return [
        index_name
        for index_name in index_names
        if index_name not in indexes_matching
    ]


async def get_old_index_names(session, es_endpoint):
    results = await es_request_non_200_exception(
        session=session,
        endpoint=es_endpoint,
        method='GET',
        path=f'/_aliases',
        query_string='',
        content_type='application/json',
        payload=b'',
    )
    indexes = await results.json()

    without_alias = [
        index_name
        for index_name, index_details in indexes.items()
        if index_name.startswith(f'{ALIAS}_') and not index_details['aliases']
    ]
    with_alias = [
        index_name
        for index_name, index_details in indexes.items()
        if index_name.startswith(f'{ALIAS}_') and index_details['aliases']
    ]
    return without_alias, with_alias


async def add_remove_aliases_atomically(session, es_endpoint, index_name, feed_unique_id):
    remove_pattern = f'{ALIAS}__feed_id_{feed_unique_id}__*'
    actions = json.dumps({
        'actions': [
            {'remove': {'index': remove_pattern, 'alias': ALIAS}},
            {'add': {'index': index_name, 'alias': ALIAS}}
        ]
    }).encode('utf-8')

    await es_request_non_200_exception(
        session=session,
        endpoint=es_endpoint,
        method='POST',
        path=f'/_aliases',
        query_string='',
        content_type='application/json',
        payload=actions,
    )


async def delete_indexes(session, es_endpoint, index_names):
    for index_name in index_names:
        await es_request_non_200_exception(
            session=session,
            endpoint=es_endpoint,
            method='DELETE',
            path=f'/{index_name}',
            query_string='',
            content_type='application/json',
            payload=b'',
        )


async def create_index(session, es_endpoint, index_name):
    index_definition = json.dumps({
        'settings': {
            'index': {
                'number_of_shards': 4,
                'number_of_replicas': 1,
            }
        }
    }).encode('utf-8')
    await es_request_non_200_exception(
        session=session,
        endpoint=es_endpoint,
        method='PUT',
        path=f'/{index_name}',
        query_string='',
        content_type='application/json',
        payload=index_definition,
    )


async def refresh_index(session, es_endpoint, index_name):
    await es_request_non_200_exception(
        session=session,
        endpoint=es_endpoint,
        method='POST',
        path=f'/{index_name}/_refresh',
        query_string='',
        content_type='application/json',
        payload=b'',
    )


async def create_mapping(session, es_endpoint, index_name):
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
    await es_request_non_200_exception(
        session=session,
        endpoint=es_endpoint,
        method='PUT',
        path=f'/{index_name}/_mapping/_doc',
        query_string='',
        content_type='application/json',
        payload=mapping_definition,
    )


async def es_search_new_scroll(_, __, query):
    return f'/{ALIAS}/_search', 'scroll=15s', query


async def es_search_existing_scroll(redis_client, match_info, _):
    # This is not wrapped in a try/except. This function should only be
    # called if public_scroll_id is in match_info, and there is some server
    # error if this isn't present, and so bubbling up and resulting in a 500
    # is appropriate if a KeyError is thrown
    public_scroll_id = match_info['public_scroll_id']

    private_scroll_id = await redis_client.get(f'private-scroll-id-{public_scroll_id}')
    if private_scroll_id is None:
        # It can be argued that this function shouldn't have knowledge that
        # it's called from a HTTP request. However, that _is_ its only use,
        # so KISS, and not introduce more layers unless needed
        raise HTTPNotFound(text='Scroll ID not found.')

    return '/_search/scroll', 'scroll=30s', json.dumps({
        'scroll_id': private_scroll_id.decode('utf-8'),
    }).encode('utf-8')


async def es_search(session, es_endpoint, path, query_string, body,
                    content_type, to_public_scroll_url):
    results = await es_request(
        session=session,
        endpoint=es_endpoint,
        method='GET',
        path=path,
        query_string=query_string,
        content_type=content_type,
        payload=body,
    )

    response = await results.json()
    return \
        (await activities(response, to_public_scroll_url), 200) if results.status == 200 else \
        (response, results.status)


async def activities(elasticsearch_reponse, to_public_scroll_url):
    elasticsearch_hits = elasticsearch_reponse['hits'].get('hits', [])
    private_scroll_id = elasticsearch_reponse['_scroll_id']
    next_dict = {
        'next': await to_public_scroll_url(private_scroll_id)
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


@async_counter
@async_timer
async def es_bulk(session, es_endpoint, items, **_):
    app_logger = logging.getLogger('activity-stream')

    if not items:
        app_logger.debug('No ES items. Skipping')
        return

    app_logger.debug('Converting feed to ES bulk ingest commands...')
    es_bulk_contents = ('\n'.join(flatten([
        [json.dumps(item['action_and_metadata'], sort_keys=True),
         json.dumps(item['source'], sort_keys=True)]
        for item in items
    ])) + '\n').encode('utf-8')
    app_logger.debug('Converting to ES bulk ingest commands: done')

    app_logger.debug('POSTing bulk import to ES...')
    await es_request_non_200_exception(
        session=session, endpoint=es_endpoint,
        method='POST', path='/_bulk', query_string='',
        content_type='application/x-ndjson', payload=es_bulk_contents,
    )
    app_logger.debug('Pushing to ES: done')


async def es_searchable_total(session, es_endpoint):
    searchable_result = await es_request_non_200_exception(
        session=session,
        endpoint=es_endpoint,
        method='GET',
        path=f'/{ALIAS}/_count',
        query_string='ignore_unavailable=true',
        content_type='application/json',
        payload=b'',
    )
    return (await searchable_result.json())['count']


async def es_nonsearchable_total(session, es_endpoint):
    nonsearchable_result = await es_request_non_200_exception(
        session=session,
        endpoint=es_endpoint,
        method='GET',
        path=f'/{ALIAS}_*,-*{ALIAS}/_count',
        query_string='ignore_unavailable=true',
        content_type='application/json',
        payload=b'',
    )
    return (await nonsearchable_result.json())['count']


async def es_feed_activities_total(session, es_endpoint, feed_id):
    nonsearchable_result = await es_request_non_200_exception(
        session=session,
        endpoint=es_endpoint,
        method='GET',
        path=f'/{ALIAS}__feed_id_{feed_id}__*,-*{ALIAS}/_count',
        query_string='ignore_unavailable=true',
        content_type='application/json',
        payload=b'',
    )
    nonsearchable = (await nonsearchable_result.json())['count']

    total_result = await es_request_non_200_exception(
        session=session,
        endpoint=es_endpoint,
        method='GET',
        path=f'/{ALIAS}__feed_id_{feed_id}__*/_count',
        query_string='ignore_unavailable=true',
        content_type='application/json',
        payload=b'',
    )
    searchable = max((await total_result.json())['count'] - nonsearchable, 0)

    return searchable, nonsearchable


async def es_min_verification_age(session, es_endpoint):
    payload = json.dumps({
        'size': 0,
        'aggs': {
            'verifier_activities': {
                'filter': {
                    'term': {
                        'object.type': 'dit:activityStreamVerificationFeed:Verifier'
                    }
                },
                'aggs': {
                    'max_published': {
                        'max': {
                            'field': 'published'
                        }
                    }
                }
            }
        }
    }).encode('utf-8')
    result = await es_request_non_200_exception(
        session=session,
        endpoint=es_endpoint,
        method='GET',
        path=f'/{ALIAS}/_search',
        query_string='ignore_unavailable=true',
        content_type='application/json',
        payload=payload,
    )
    result_dict = await result.json()
    try:
        max_published = int(result_dict['aggregations']
                            ['verifier_activities']['max_published']['value'] / 1000)
        now = int(time.time())
        age = now - max_published
    except (KeyError, TypeError):
        # If there aren't any activities yet, don't error
        raise ESMetricsUnavailable()
    return age


async def es_request_non_200_exception(session, endpoint, method, path, query_string,
                                       content_type, payload):
    results = await es_request(session, endpoint, method, path, query_string,
                               content_type, payload)
    if results.status != 200:
        raise Exception(await results.text())
    return results


async def es_request(session, endpoint, method, path, query_string, content_type, payload):
    auth_headers = es_auth_headers(
        endpoint=endpoint, method=method,
        path=path, query_string=query_string,
        content_type=content_type, payload=payload,
    )

    url = endpoint['base_url'] + path + (('?' + query_string) if query_string != '' else '')
    result = await session.request(
        method, url,
        data=payload, headers={**{'Content-Type': content_type}, **auth_headers}
    )
    # Without this, after some number of requests, they end up hanging
    await result.read()
    return result


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
            canonical_uri = urllib.parse.quote(path)
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


class ESMetricsUnavailable(Exception):
    pass
