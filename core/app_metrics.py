import time

from prometheus_client import (
    Summary,
    PlatformCollector,
    ProcessCollector,
)


METRICS_CONF = [
    (Summary, 'ingest_single_feed_duration_seconds',
     'Time to ingest a single feed in seconds', ['feed_unique_id'])
]


def get_metrics(registry):
    PlatformCollector(registry=registry)
    ProcessCollector(registry=registry)
    return {
        # The metric classes are constructed via decorators which
        # result in pylint giving a false positive
        # pylint: disable=unexpected-keyword-arg
        name: metric_class(name, description, labels, registry=registry)
        for metric_class, name, description, labels in METRICS_CONF
    }


def async_timer(metric, labels):

    def async_timer_decorator(coroutine):

        async def wrapper(*args, **kwargs):
            start_counter = time.perf_counter()
            try:
                return await coroutine(*args, **kwargs)
            finally:
                end_counter = time.perf_counter()
                metric.labels(*labels).observe(end_counter - start_counter)

        return wrapper

    return async_timer_decorator
