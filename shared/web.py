from aiohttp import web


def authenticate_by_ip(app_logger, incorrect, ip_whitelist):

    @web.middleware
    async def _authenticate_by_ip(request, handler):
        if 'X-Forwarded-For' not in request.headers:
            app_logger.warning(
                'Failed authentication: no X-Forwarded-For header passed'
            )
            raise web.HTTPUnauthorized(text=incorrect)

        # PaaS appends 2 IPs, where the IP connected from is the first of the two
        ip_addesses = request.headers['X-Forwarded-For'].split(',')
        if len(ip_addesses) < 2:
            app_logger.warning(
                'Failed authentication: the X-Forwarded-For header does not '
                'contain enough IP addresses'
            )
            raise web.HTTPUnauthorized(text=incorrect)

        remote_address = ip_addesses[-2].strip()

        if remote_address not in ip_whitelist:
            app_logger.warning(
                'Failed authentication: the IP address derived from the '
                'X-Forwarded-For header is not in the whitelist'
            )
            raise web.HTTPUnauthorized(text=incorrect)

        return await handler(request)

    return _authenticate_by_ip
