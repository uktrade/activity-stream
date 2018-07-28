import time

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    PlatformCollector,
    ProcessCollector,
)

from .app_utils import (
    extract_keys,
)


METRICS_CONF = [
    (Summary, 'ingest_single_feed_duration_seconds',
     'Time to ingest a single feed in seconds', ['feed_unique_id', 'status']),
    (Histogram, 'ingest_outgoing_requests_duration_seconds',
     'Time for outgoing ingest requests to complete', ['feed_unique_id', 'status']),
    (Gauge, 'ingest_inprogress_ingests_total',
     'The number of inprogress ingests', []),
    (Counter, 'ingest_activities_nonunique_total',
     'The number of nonunique activities ingested', ['feed_unique_id']),
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


def async_inprogress(coroutine):

    async def wrapper(*args, **kwargs):
        kwargs_to_pass, (metric, ) = extract_keys(
            kwargs,
            ['_async_inprogress'],
        )

        try:
            metric.inc()
            return await coroutine(*args, **kwargs_to_pass)
        finally:
            metric.dec()

    return wrapper


def async_timer(coroutine):

    async def wrapper(*args, **kwargs):
        kwargs_to_pass, (metric, labels, is_running) = extract_keys(
            kwargs,
            ['_async_timer', '_async_timer_labels', '_async_timer_is_running'],
        )

        start_counter = time.perf_counter()
        try:
            response = await coroutine(*args, **kwargs_to_pass)
            status = 'success'
            return response
        except BaseException:
            status = 'failure'
            raise
        finally:
            end_counter = time.perf_counter()
            if is_running():
                metric.labels(*(labels + [status])).observe(end_counter - start_counter)

    return wrapper


def async_counter(coroutine):

    async def wrapper(*args, **kwargs):
        kwargs_to_pass, (metric, labels, increment_by) = extract_keys(
            kwargs,
            ['_async_counter', '_async_counter_labels', '_async_counter_increment_by'],
        )

        response = await coroutine(*args, **kwargs_to_pass)
        metric.labels(*labels).inc(increment_by)
        return response

    return wrapper
