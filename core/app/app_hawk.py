from base64 import (
    b64encode,
)
from datetime import (
    datetime,
)
import hashlib
import hmac
import os


def get_hawk_header(access_key_id, secret_access_key, method, host, port, path, content_type,
                    content):
    canonical_payload = \
        b'hawk.1.payload' + b'\n' + \
        content_type + b'\n' + \
        content + b'\n'
    payload_hash = base64_digest(canonical_payload)

    timestamp = str(int(datetime.now().timestamp()))
    nonce = b64encode(os.urandom(5)).decode('utf-8')[:6]

    canonical_request = \
        f'hawk.1.header\n{timestamp}\n{nonce}\n{method}\n{path}\n{host}\n{port}\n' \
        f'{payload_hash}\n\n'
    mac = base64_mac(secret_access_key.encode('utf-8'), canonical_request.encode('utf-8'))

    header = \
        f'Hawk mac="{mac}", hash="{payload_hash}", id="{access_key_id}", ts="{timestamp}", ' \
        f'nonce="{nonce}"'

    return header


def base64_digest(data):
    return b64encode(hashlib.sha256(data).digest()).decode('utf-8')


def base64_mac(key, data):
    return b64encode(hmac.new(key, data, hashlib.sha256).digest()).decode('utf-8')
