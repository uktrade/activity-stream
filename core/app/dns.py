import asyncio
import socket

from aiodnsresolver import (
    TYPES,
    DnsError,
    DnsRecordDoesNotExist,
    Resolver,
    mix_case,
)
import aiohttp

from .metrics import (
    metric_timer,
)


class AioHttpDnsResolver(aiohttp.abc.AbstractResolver):
    def __init__(self, metrics):
        super().__init__()

        async def transform_fqdn(fqdn):
            return \
                fqdn if fqdn.endswith(b'.apps.internal') else \
                await mix_case(fqdn)

        self.metrics = metrics
        self.resolver, self.clear = Resolver(transform_fqdn=transform_fqdn)

    # pylint: disable=arguments-differ
    async def resolve(self, host, port=0, family=socket.AF_INET):
        # Use ipv4 unless requested otherwise
        # This is consistent with the default aiohttp + aiodns AsyncResolver
        record_type = \
            TYPES.AAAA if family == socket.AF_INET6 else \
            TYPES.A

        try:
            with metric_timer(self.metrics['dns_request_duration_seconds'], [host]):
                ip_addresses = await self.resolver(host, record_type)
        except DnsRecordDoesNotExist as does_not_exist:
            raise OSError(0, '{} does not exist'.format(host)) from does_not_exist
        except DnsError as dns_error:
            raise OSError(0, '{} failed to resolve'.format(host)) from dns_error

        min_expires_at = min(ip_address.expires_at for ip_address in ip_addresses)
        ttl = max(0, min_expires_at - asyncio.get_event_loop().time())
        self.metrics['dns_ttl'].labels(host).set(ttl)

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
