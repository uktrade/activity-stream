import asyncio
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
    (Summary, 'ingest_feeds_duration_seconds',
     'Time to ingest all feeds in seconds', ['status']),
    (Summary, 'ingest_feed_duration_seconds',
     'Time to ingest all pages of a feed in seconds', ['feed_unique_id', 'status']),
    (Histogram, 'ingest_page_duration_seconds',
     'Time for a page of data to be ingested in seconds', ['feed_unique_id', 'stage', 'status']),
    (Gauge, 'ingest_inprogress_ingests_total',
     'The number of inprogress ingests', []),
    (Counter, 'ingest_activities_nonunique_total',
     'The number of nonunique activities ingested', ['feed_unique_id']),
    (Gauge, 'elasticsearch_activities_total',
     'The number of activities stored in Elasticsearch', ['searchable']),
    (Gauge, 'elasticsearch_feed_activities_total',
     'The number of activities from a feed stored in Elasticsearch',
     ['feed_unique_id', 'searchable']),
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
        kwargs_to_pass, (metric, labels) = extract_keys(
            kwargs,
            ['_async_timer', '_async_timer_labels'],
        )

        start_counter = time.perf_counter()
        try:
            response = await coroutine(*args, **kwargs_to_pass)
            status = 'success'
            return response
        except asyncio.CancelledError:
            status = 'cancelled'
            raise
        except BaseException:
            status = 'failure'
            raise
        finally:
            end_counter = time.perf_counter()
            metric.labels(*(labels + [status])).observe(end_counter - start_counter)

    return wrapper


def async_counter(coroutine):

    async def wrapper(*args, **kwargs):
        kwargs_to_pass, (metric, labels, increment_by) = extract_keys(
            kwargs,
            ['_async_counter', '_async_counter_labels', '_async_counter_increment_by'],
        )

        try:
            response = await coroutine(*args, **kwargs_to_pass)
            increment_by_value = increment_by
            return response
        except BaseException:
            increment_by_value = 0
            raise
        finally:
            metric.labels(*labels).inc(increment_by_value)

    return wrapper
