import time

import ujson

from .http import (
    http_make_request,
)
from .logger import (
    logged,
)

from . import settings

ALIAS_ACTIVITIES = 'activities'
ALIAS_OBJECTS = 'objects'


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
    _, result_bytes = await es_request_non_200_exception(
        context=context,
        method='GET',
        path=f'/{ALIAS_ACTIVITIES}/_search',
        query={'ignore_unavailable': 'true'},
        headers={'Content-Type': 'application/json'},
        payload=payload,
    )
    result_dict = ujson.loads(result_bytes.decode('utf-8'))
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
    results, results_bytes = await es_request(context, method, path, query, headers, payload)
    if results.status not in [200, 201]:
        raise Exception(results_bytes.decode('utf-8'))
    return results, results_bytes


async def es_request(context, method, path, query, headers, payload):
    with logged(
        context.logger, 'Elasticsearch request (%s) (%s) (%s) (%s)',
        [settings.ES_URI, method, path, query],
    ):
        query_string = '&'.join([key + '=' + query[key] for key in query.keys()])
        return await http_make_request(
            context.session, context.metrics, method,
            settings.ES_URI + path + (('?' + query_string) if query_string != '' else ''),
            data=payload, headers=headers,
        )


class ESMetricsUnavailable(Exception):
    pass
