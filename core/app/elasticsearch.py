import datetime
import time

from aiohttp.web import (
    HTTPNotFound,
)
import ujson

from .logger import (
    logged,
)
from .redis import (
    get_private_scroll_id,
)
from .utils import (
    flatten,
    flatten_generator,
    random_url_safe,
)
from . import settings

ALIAS = 'activities'


def get_new_index_name(feed_unique_id):
    today = datetime.date.today().isoformat()
    now = str(datetime.datetime.now().timestamp()).split('.')[0]
    unique = random_url_safe(8)

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


async def get_old_index_names(context):
    with logged(context.logger, 'Finding existing index names', []):
        results = await es_request_non_200_exception(
            context=context,
            method='GET',
            path=f'/_aliases',
            query={},
            headers={'Content-Type': 'application/json'},
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
        names = without_alias, with_alias
        context.logger.debug('Finding existing index names... (%s)', names)
        return names


async def add_remove_aliases_atomically(context, index_name, feed_unique_id):
    with logged(context.logger, 'Atomically flipping {ALIAS} alias to (%s)',
                [feed_unique_id]):
        remove_pattern = f'{ALIAS}__feed_id_{feed_unique_id}__*'
        actions = ujson.dumps({
            'actions': [
                {'remove': {'index': remove_pattern, 'alias': ALIAS}},
                {'add': {'index': index_name, 'alias': ALIAS}}
            ]
        }).encode('utf-8')

        await es_request_non_200_exception(
            context=context,
            method='POST',
            path=f'/_aliases',
            query={},
            headers={'Content-Type': 'application/json'},
            payload=actions,
        )


async def delete_indexes(context, index_names):
    with logged(context.logger, 'Deleting indexes (%s)', [index_names]):
        for index_name in index_names:
            await es_request_non_200_exception(
                context=context,
                method='DELETE',
                path=f'/{index_name}',
                query={},
                headers={'Content-Type': 'application/json'},
                payload=b'',
            )


async def create_index(context, index_name):
    with logged(context.logger, 'Creating index (%s)', [index_name]):
        index_definition = ujson.dumps({
            'settings': {
                'index': {
                    'number_of_shards': 4,
                    'number_of_replicas': 1,
                    'refresh_interval': '-1',
                }
            },
            'mappings': {
                '_doc': {
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
                },
            },
        }, escape_forward_slashes=False, ensure_ascii=False).encode('utf-8')
        await es_request_non_200_exception(
            context=context,
            method='PUT',
            path=f'/{index_name}',
            query={},
            headers={'Content-Type': 'application/json'},
            payload=index_definition,
        )


async def refresh_index(context, index_name):
    with logged(context.logger, 'Refreshing index (%s)', [index_name]):
        await es_request_non_200_exception(
            context=context,
            method='POST',
            path=f'/{index_name}/_refresh',
            query={'ignore_unavailable': 'true'},
            headers={'Content-Type': 'application/json'},
            payload=b'',
        )


async def es_search_new_scroll(_, __, query):
    return f'/{ALIAS}/_search', {'scroll': '15s'}, query


async def es_search_existing_scroll(context, match_info, _):
    # This is not wrapped in a try/except. This function should only be
    # called if public_scroll_id is in match_info, and there is some server
    # error if this isn't present, and so bubbling up and resulting in a 500
    # is appropriate if a KeyError is thrown
    public_scroll_id = match_info['public_scroll_id']
    private_scroll_id = await get_private_scroll_id(context, public_scroll_id)

    if private_scroll_id is None:
        # It can be argued that this function shouldn't have knowledge that
        # it's called from a HTTP request. However, that _is_ its only use,
        # so KISS, and not introduce more layers unless needed
        raise HTTPNotFound(text='Scroll ID not found.')

    return '/_search/scroll', {'scroll': '30s'}, ujson.dumps({
        'scroll_id': private_scroll_id.decode('utf-8'),
    }, escape_forward_slashes=False, ensure_ascii=False).encode('utf-8')


async def es_search(context, path, query, body, headers, to_public_scroll_url):
    results = await es_request(
        context=context,
        method='GET',
        path=path,
        query=query,
        headers=headers,
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


async def es_bulk(context, items):
    with logged(context.logger, 'Pushing (%s) items into Elasticsearch', [len(items)]):
        if not items:
            return

        with logged(context.logger, 'Converting to Elasticsearch bulk ingest commands', []):
            es_bulk_contents = ''.join(flatten_generator(
                [ujson.dumps(item['action_and_metadata'], sort_keys=True,
                             escape_forward_slashes=False, ensure_ascii=False),
                 '\n',
                 ujson.dumps(item['source'], sort_keys=True,
                             escape_forward_slashes=False, ensure_ascii=False),
                 '\n']
                for item in items
            )).encode('utf-8')

        with logged(context.logger, 'POSTing bulk ingest to Elasticsearch', []):
            await es_request_non_200_exception(
                context=context, method='POST', path='/_bulk', query={},
                headers={'Content-Type': 'application/x-ndjson'}, payload=es_bulk_contents,
            )


async def es_searchable_total(context):
    # This metric is expected to be available
    searchable_result = await es_request_non_200_exception(
        context=context,
        method='GET',
        path=f'/{ALIAS}/_count',
        query={'ignore_unavailable': 'true'},
        headers={'Content-Type': 'application/json'},
        payload=b'',
    )
    return (ujson.loads(await searchable_result.text()))['count']


async def es_nonsearchable_total(context):
    nonsearchable_result = await es_maybe_unvailable_metrics(
        context=context,
        method='GET',
        path=f'/{ALIAS}_*,-*{ALIAS}/_count',
        query={'ignore_unavailable': 'true'},
        headers={'Content-Type': 'application/json'},
        payload=b'',
    )
    return ujson.loads(await nonsearchable_result.text())['count']


async def es_feed_activities_total(context, feed_id):
    nonsearchable_result = await es_maybe_unvailable_metrics(
        context=context,
        method='GET',
        path=f'/{ALIAS}__feed_id_{feed_id}__*,-*{ALIAS}/_count',
        query={'ignore_unavailable': 'true'},
        headers={'Content-Type': 'application/json'},
        payload=b'',
    )
    nonsearchable = ujson.loads(await nonsearchable_result.text())['count']

    total_result = await es_maybe_unvailable_metrics(
        context=context,
        method='GET',
        path=f'/{ALIAS}__feed_id_{feed_id}__*/_count',
        query={'ignore_unavailable': 'true'},
        headers={'Content-Type': 'application/json'},
        payload=b'',
    )
    searchable = max(ujson.loads(await total_result.text())['count'] - nonsearchable, 0)

    return searchable, nonsearchable


async def es_min_verification_age(context):
    payload = ujson.dumps({
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
    }, escape_forward_slashes=False, ensure_ascii=False).encode('utf-8')
    result = await es_request_non_200_exception(
        context=context,
        method='GET',
        path=f'/{ALIAS}/_search',
        query={'ignore_unavailable': 'true'},
        headers={'Content-Type': 'application/json'},
        payload=payload,
    )
    result_dict = ujson.loads(await result.text())
    try:
        max_published = int(result_dict['aggregations']
                            ['verifier_activities']['max_published']['value'] / 1000)
        now = int(time.time())
        age = now - max_published
    except (KeyError, TypeError):
        # If there aren't any activities yet, don't error
        raise ESMetricsUnavailable()
    return age


async def es_request_non_200_exception(context, method, path, query, headers, payload):
    results = await es_request(context, method, path, query, headers, payload)
    if results.status not in [200, 201]:
        raise Exception(await results.text())
    return results


async def es_maybe_unvailable_metrics(context, method, path, query, headers, payload):
    ''' Some metrics may be unavailable since they query newly created indexes '''
    results = await es_request(context, method, path, query, headers, payload)
    if results.status == 503:
        raise ESMetricsUnavailable()
    if results.status != 200:
        raise Exception(await results.text())
    return results


async def es_request(context, method, path, query, headers, payload):
    with logged(
        context.logger, 'Elasticsearch request (%s) (%s) (%s) (%s)',
        [settings.ES_URI, method, path, query],
    ):
        query_string = '&'.join([key + '=' + query[key] for key in query.keys()])
        async with context.session.request(
            method, settings.ES_URI + path + (('?' + query_string) if query_string != '' else ''),
            data=payload, headers=headers,
        ) as result:
            # Without this, after some number of requests, they end up hanging
            await result.read()
            return result


class ESMetricsUnavailable(Exception):
    pass
