import asyncio
import contextlib
import time

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    Summary,
    PlatformCollector,
    ProcessCollector,
)


METRICS_CONF = [
    (Summary, 'ingest_feed_duration_seconds',
     'Time to ingest all pages of a feed in seconds',
     ['feed_unique_id', 'ingest_type', 'status']),
    (Histogram, 'ingest_page_duration_seconds',
     'Time for a page of data to be ingested in seconds',
     ['feed_unique_id', 'ingest_type', 'stage', 'status']),
    (Gauge, 'ingest_inprogress_ingests_total',
     'The number of inprogress ingests', []),
    (Counter, 'ingest_activities_nonunique_total',
     'The number of nonunique activities ingested', ['feed_unique_id', 'ingest_type']),
    (Gauge, 'elasticsearch_activities_total',
     'The number of activities stored in Elasticsearch', ['searchable']),
    (Gauge, 'elasticsearch_feed_activities_total',
     'The number of activities from a feed stored in Elasticsearch',
     ['feed_unique_id', 'searchable']),
    # Only need verification, but keeping it consistent with other metrics
    (Gauge, 'elasticsearch_activities_age_minimum_seconds',
     'The minimum age of activites from a feed stored in Elasticsearch in seconds',
     ['feed_unique_id']),
    (Summary, 'elasticsearch_refresh_duration_seconds',
     'Time to refresh an index',
     ['feed_unique_id', 'ingest_type']),
    (Summary, 'http_request_duration_seconds',
     'Time to make a http request',
     ['host', 'status']),
    (Counter, 'http_request_completed_total',
     'The number of HTTP requests completed', ['host', 'code']),
    (Counter, 'http_response_body_bytes',
     'The bytes receives in an HTTP response', ['host', 'code']),
    (Summary, 'dns_request_duration_seconds',
     'Time to make a possibly-from-cache DNS request',
     ['host', 'status']),
    (Gauge, 'dns_ttl',
     'The TTL of A or AAAA records just after a DNS request',
     ['host']),
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


@contextlib.contextmanager
def metric_inprogress(metric):
    try:
        metric.inc()
        yield
    finally:
        metric.dec()


@contextlib.contextmanager
def metric_timer(metric, labels):
    start_counter = time.perf_counter()
    try:
        yield
        status = 'success'
    except asyncio.CancelledError:
        status = 'cancelled'
        raise
    except BaseException:
        status = 'failure'
        raise
    finally:
        end_counter = time.perf_counter()
        metric.labels(*(labels + [status])).observe(end_counter - start_counter)


@contextlib.contextmanager
def metric_counter(metric, labels, increment_by):
    try:
        yield
        increment_by_value = increment_by
    except BaseException:
        increment_by_value = 0
        raise
    finally:
        metric.labels(*labels).inc(increment_by_value)
