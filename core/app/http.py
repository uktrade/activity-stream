import yarl

from .metrics import (
    metric_timer,
)


async def http_make_request(session, metrics, method, url, data, headers, timeout=None):
    parsed_url = yarl.URL(url)

    with metric_timer(metrics['http_request_duration_seconds'], [parsed_url.host]):
        async with session.request(method, parsed_url, data=data, headers=headers,
                                   timeout=timeout) as result:
            # We must read the body before the connection is closed, which can
            # be on exit of the context manager
            await result.read()

            metrics['http_request_completed_total'] \
                .labels(parsed_url.host, str(result.status)).inc(1)
            metrics['http_response_body_bytes'] \
                .labels(parsed_url.host, str(result.status)).inc(len(result._body))

            return result
