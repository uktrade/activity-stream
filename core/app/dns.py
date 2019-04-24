import socket

from aiodnsresolver import (
    TYPES,
    ResolverError,
    DoesNotExist,
    Resolver,
    mix_case,
)
import aiohttp


class AioHttpDnsResolver(aiohttp.abc.AbstractResolver):
    def __init__(self):
        super().__init__()

        async def transform_fqdn(fqdn):
            return \
                fqdn if fqdn.endswith(b'.apps.internal') else \
                await mix_case(fqdn)

        self.resolver, self.clear = Resolver(transform_fqdn=transform_fqdn)

    # pylint: disable=arguments-differ
    async def resolve(self, host, port=0, family=socket.AF_INET):
        # Use ipv4 unless requested otherwise
        # This is consistent with the default aiohttp + aiodns AsyncResolver
        record_type = \
            TYPES.AAAA if family == socket.AF_INET6 else \
            TYPES.A

        try:
            ip_addresses = await self.resolver(host, record_type)
        except DoesNotExist as does_not_exist:
            raise OSError(0, '{} does not exist'.format(host)) from does_not_exist
        except ResolverError as resolver_error:
            raise OSError(0, '{} failed to resolve'.format(host)) from resolver_error

        return [{
            'hostname': host,
            'host': str(ip_address),
            'port': port,
            'family': family,
            'proto': socket.IPPROTO_TCP,
            'flags': socket.AI_NUMERICHOST,
        } for ip_address in ip_addresses]

    async def close(self):
        self.clear()
