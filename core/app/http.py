import yarl

from .metrics import (
    metric_counter,
    metric_timer,
)


async def http_make_request(context, method, url, data, headers):
    metrics = context.metrics
    host = yarl.URL(url).host

    with metric_timer(metrics['http_request_duration_seconds'], [host]):
        async with context.session.request(method, url, data=data, headers=headers) as result:
            # Without this, after some number of requests, they end up hanging
            result_bytes = await result.read()

            with metric_counter(metrics['http_request_completed_total'],
                                [host, str(result.status)], 1):
                # The counter is a context manager, but in this case we have nothing
                # to wrap, we just want to increment the counter
                pass

            return result, result_bytes
