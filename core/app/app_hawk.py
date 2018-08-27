from base64 import (
    b64encode,
)
from datetime import (
    datetime,
)
import hashlib
import hmac
import os
import re

from .app_redis import (
    set_nonce_nx,
)


def get_hawk_header(access_key_id, secret_access_key, method, host, port, path, content_type,
                    content):
    payload_hash = get_payload_hash(content_type, content)

    timestamp = str(int(datetime.now().timestamp()))
    nonce = b64encode(os.urandom(5)).decode('utf-8')[:6]
    mac = get_mac(secret_access_key, timestamp, nonce, method, path, host, port, payload_hash)

    header = \
        f'Hawk mac="{mac}", hash="{payload_hash}", id="{access_key_id}", ts="{timestamp}", ' \
        f'nonce="{nonce}"'

    return header


async def authenticate_hawk_header(context, nonce_expire, lookup_credentials,
                                   header, method, host, port, path, content_type, content):

    is_valid_header = re.match(r'^Hawk (((?<="), )?[a-z]+="[^"]*")*$', header)
    if not is_valid_header:
        return False, 'Invalid header', {}

    parsed_header = dict(re.findall(r'([a-z]+)="([^"]+)"', header))

    required_fields = ['ts', 'hash', 'mac', 'nonce', 'id']
    missing_fields = [
        field for field in required_fields
        if field not in parsed_header
    ]
    if missing_fields:
        return False, f'Missing {missing_fields[0]}', None

    if not re.match(r'^\d+$', parsed_header['ts']):
        return False, 'Invalid ts', None

    matching_credentials = lookup_credentials(parsed_header['id'])
    if not matching_credentials:
        return False, 'Unidentified id', None

    correct_payload_hash = get_payload_hash(content_type, content)
    correct_mac = get_mac(
        secret_access_key=matching_credentials['key'],
        timestamp=parsed_header['ts'],
        nonce=parsed_header['nonce'],
        method=method,
        path=path,
        host=host,
        port=port,
        payload_hash=correct_payload_hash,
    )

    if not hmac.compare_digest(correct_payload_hash, parsed_header['hash']):
        return False, 'Invalid hash', None

    if not abs(int(datetime.now().timestamp()) - int(parsed_header['ts'])) <= 60:
        return False, 'Stale ts', None

    if not hmac.compare_digest(correct_mac, parsed_header['mac']):
        return False, 'Invalid mac', None

    if not await is_nonce_available(context, parsed_header['nonce'], matching_credentials['id'],
                                    nonce_expire):
        return False, 'Invalid nonce', None

    return True, '', matching_credentials


async def is_nonce_available(context, nonce, access_key_id, nonce_expire):
    nonce_key = f'nonce-{access_key_id}-{nonce}'
    redis_response = await set_nonce_nx(context, nonce_key, nonce_expire)
    nonce_available = redis_response == b'OK'
    return nonce_available


def get_payload_hash(content_type, content):
    canonical_payload = \
        b'hawk.1.payload' + b'\n' + \
        content_type + b'\n' + \
        content + b'\n'
    return base64_digest(canonical_payload)


def get_mac(secret_access_key, timestamp, nonce, method, path, host, port, payload_hash):
    canonical_request = \
        f'hawk.1.header\n{timestamp}\n{nonce}\n{method}\n{path}\n{host}\n{port}\n' \
        f'{payload_hash}\n\n'
    return base64_mac(secret_access_key.encode('utf-8'), canonical_request.encode('utf-8'))


def base64_digest(data):
    return b64encode(hashlib.sha256(data).digest()).decode('utf-8')


def base64_mac(key, data):
    return b64encode(hmac.new(key, data, hashlib.sha256).digest()).decode('utf-8')
