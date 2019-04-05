import yarl

from .metrics import (
    metric_counter,
    metric_timer,
)


async def http_make_request(session, metrics, method, url, data, headers):
    parsed_url = yarl.URL(url)

    with metric_timer(metrics['http_request_duration_seconds'], [parsed_url.host]):
        async with session.request(method, parsed_url, data=data, headers=headers) as result:
            # We must read the body before the connection is closed, which can
            # be on exit of the context manager
            await result.read()

            with metric_counter(metrics['http_request_completed_total'],
                                [parsed_url.host, str(result.status)], 1):
                # The counter is a context manager, but in this case we have nothing
                # to wrap, we just want to increment the counter
                pass

            return result
