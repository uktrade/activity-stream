import time

from .http import (
    http_make_request,
)
from .logger import (
    logged,
)
from .utils import (
    json_dumps,
    json_loads,
)

from . import settings

ALIAS_ACTIVITIES = 'activities'
ALIAS_ACTIVITIES_SCHEMAS = 'activities_schemas'
ALIAS_OBJECTS = 'objects'
ALIAS_OBJECTS_SCHEMAS = 'objects_schemas'


class ESNon200Exception(Exception):
    pass


async def es_min_verification_age(context):
    payload = json_dumps({
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
    })
    result = await es_request_non_200_exception(
        context=context,
        method='GET',
        path=f'/{ALIAS_ACTIVITIES}/_search',
        query={'ignore_unavailable': 'true'},
        headers={'Content-Type': 'application/json'},
        payload=payload,
    )
    result_dict = json_loads(result._body)
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
        raise ESNon200Exception(results._body.decode('utf-8'), results.status)
    return results


async def es_request(context, method, path, query, headers, payload):
    with logged(
            context.logger.debug, context.logger.warning,
            'Elasticsearch request (%s) (%s) (%s) (%s)',
            [settings.ES_URI, method, path, query],
    ):
        query_string = '&'.join([key + '=' + query[key] for key in query.keys()])
        return await http_make_request(
            context.session, context.metrics, method,
            settings.ES_URI + path + (('?' + query_string) if query_string != '' else ''),
            data=payload, headers=headers,
        )


def es_mappings(mappings):
    return \
        {'_doc': mappings} if settings.ES_VERSION == '6.x' else \
        mappings


class ESMetricsUnavailable(Exception):
    pass
