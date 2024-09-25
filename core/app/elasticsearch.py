from datetime import datetime
import hashlib
import hmac
import time
import urllib.parse

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
    async with context.es_semaphore:
        with logged(
                context.logger.debug, context.logger.warning,
                'Elasticsearch request (%s) (%s) (%s) (%s)',
                [settings.ES_URI, method, path, query],
        ):
            # It's not ideal, but we have a mix of environments. Some use AWS authentication, and
            # some basic (and so with credentials in ES_URI)
            if settings.ES_AWS_ACCESS_KEY_ID:
                host = urllib.parse.urlsplit(settings.ES_URI).hostname
                headers = dict(aws_sigv4_headers(
                    settings.ES_AWS_ACCESS_KEY_ID, settings.ES_AWS_SECRET_ACCESS_KEY,
                    settings.ES_AWS_REGION,
                    tuple(headers.items()), 'es', host, method, path, tuple(query.items()), payload
                ))

            query_string = '&'.join([key + '=' + query[key] for key in query.keys()])
            return await http_make_request(
                context.session, context.metrics, method,
                settings.ES_URI + path + (('?' + query_string) if query_string != '' else ''),
                data=payload, headers=headers,
            )


def aws_sigv4_headers(
        aws_access_key_id, aws_secret_access_key, region_name,
        pre_auth_headers, service, host, method, path, params, body):
    algorithm = 'AWS4-HMAC-SHA256'
    body_hash = hashlib.sha256(body).hexdigest()

    now = datetime.utcnow()
    amzdate = now.strftime('%Y%m%dT%H%M%SZ')
    datestamp = now.strftime('%Y%m%d')
    credential_scope = f'{datestamp}/{region_name}/{service}/aws4_request'

    pre_auth_headers_lower = tuple((
        (header_key.lower(), ' '.join(header_value.split()))
        for header_key, header_value in pre_auth_headers
    ))
    required_headers = (
        ('host', host),
        ('x-amz-content-sha256', body_hash),
        ('x-amz-date', amzdate),
    )
    headers = sorted(pre_auth_headers_lower + required_headers)
    signed_headers = ';'.join(key for key, _ in headers)

    def signature():
        def canonical_request():
            canonical_uri = urllib.parse.quote(path, safe='/~')
            quoted_params = sorted(
                (urllib.parse.quote(key, safe='~'), urllib.parse.quote(value, safe='~'))
                for key, value in params
            )
            canonical_querystring = '&'.join(f'{key}={value}' for key, value in quoted_params)
            canonical_headers = ''.join(f'{key}:{value}\n' for key, value in headers)

            return f'{method}\n{canonical_uri}\n{canonical_querystring}\n' + \
                f'{canonical_headers}\n{signed_headers}\n{body_hash}'

        def sign(key, msg):
            return hmac.new(key, msg.encode('ascii'), hashlib.sha256).digest()

        string_to_sign = f'{algorithm}\n{amzdate}\n{credential_scope}\n' + \
            hashlib.sha256(canonical_request().encode('ascii')).hexdigest()

        date_key = sign(('AWS4' + aws_secret_access_key).encode('ascii'), datestamp)
        region_key = sign(date_key, region_name)
        service_key = sign(region_key, service)
        request_key = sign(service_key, 'aws4_request')
        return sign(request_key, string_to_sign).hex()

    return (
        ('authorization', (
            f'{algorithm} Credential={aws_access_key_id}/{credential_scope}, '
            f'SignedHeaders={signed_headers}, Signature=' + signature())
         ),
        ('x-amz-date', amzdate),
        ('x-amz-content-sha256', body_hash),
    ) + pre_auth_headers


def es_mappings(mappings):
    return \
        {'_doc': mappings} if settings.ES_VERSION == '6.x' else \
        mappings


class ESMetricsUnavailable(Exception):
    pass
