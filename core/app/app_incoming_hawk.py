from hawkserver import (
    authenticate_hawk_header as _authenticate_hawk_header,
)

from .app_incoming_redis import (
    set_nonce_nx,
)


async def authenticate_hawk_header(context, nonce_expire, lookup_credentials,
                                   header, method, host, port, path, content_type, content):

    async def seen_nonce(nonce, access_key_id):
        nonce_key = f'nonce-{access_key_id}-{nonce}'
        redis_response = await set_nonce_nx(context, nonce_key, nonce_expire)
        return redis_response == b'OK'

    return await _authenticate_hawk_header(
        lookup_credentials, seen_nonce,
        header, method, host, port, path, content_type, content,
    )
