import datetime
import hashlib
import hmac
import json


async def ensure_index(session, es_endpoint):
    index_definition = json.dumps({}).encode('utf-8')
    path = '/activities'
    auth_headers = es_auth_headers(
        endpoint=es_endpoint,
        method='PUT',
        path=path,
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


def es_auth_headers(endpoint, method, path, payload):
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
            canonical_querystring = ''
            canonical_headers = \
                f'content-type:application/x-ndjson\n' + \
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
