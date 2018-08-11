from aiohttp import web

from .logger import (
    get_child_logger,
    logged,
)
from .utils import (
    random_url_safe,
)


def server_logger(logger):

    @web.middleware
    async def _server_logger(request, handler):
        child_logger = get_child_logger(logger, random_url_safe(8))
        request['logger'] = child_logger
        child_logger.debug('Receiving request %s "%s %s HTTP/%s.%s" "%s" "%s"', *(
            (
                request.remote,
                request.method,
                request.path_qs,
            ) +
            request.version +
            (
                request.headers.get('User-Agent', '-'),
                request.headers.get('X-Forwarded-For', '-'),
            )
        ))

        with logged(child_logger, 'Processing request', []):
            response = await handler(request)

        child_logger.debug(
            'Sending Response %s %s',
            response.status, response.content_length,
        )

        return response

    return _server_logger


def authenticate_by_ip(incorrect, ip_whitelist):

    @web.middleware
    async def _authenticate_by_ip(request, handler):
        if 'X-Forwarded-For' not in request.headers:
            request['logger'].warning(
                'Failed authentication: no X-Forwarded-For header passed'
            )
            raise web.HTTPUnauthorized(text=incorrect)

        # PaaS appends 2 IPs, where the IP connected from is the first of the two
        ip_addesses = request.headers['X-Forwarded-For'].split(',')
        if len(ip_addesses) < 2:
            request['logger'].warning(
                'Failed authentication: the X-Forwarded-For header does not '
                'contain enough IP addresses'
            )
            raise web.HTTPUnauthorized(text=incorrect)

        remote_address = ip_addesses[-2].strip()

        if remote_address not in ip_whitelist:
            request['logger'].warning(
                'Failed authentication: the IP address derived from the '
                'X-Forwarded-For header is not in the whitelist'
            )
            raise web.HTTPUnauthorized(text=incorrect)

        return await handler(request)

    return _authenticate_by_ip
