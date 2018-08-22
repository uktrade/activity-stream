import asyncio
import os

import aiohttp
from prometheus_client import (
    CollectorRegistry,
    generate_latest,
)
import ujson

from shared.logger import (
    get_root_logger,
    logged,
)
from shared.utils import (
    get_common_config,
    normalise_environment,
)

from .app_elasticsearch import (
    ESMetricsUnavailable,
    es_bulk,
    es_feed_activities_total,
    es_searchable_total,
    es_nonsearchable_total,
    es_min_verification_age,
    create_index,
    get_new_index_name,
    get_old_index_names,
    indexes_matching_feeds,
    indexes_matching_no_feeds,
    add_remove_aliases_atomically,
    delete_indexes,
    refresh_index,
)

from .app_feeds import (
    parse_feed_config,
)
from .app_metrics import (
    metric_counter,
    metric_inprogress,
    metric_timer,
    get_metrics,
)
from .app_raven import (
    get_raven_client,
)
from .app_redis import (
    redis_get_client,
    acquire_and_keep_lock,
    set_feed_updates_seed_url_init,
    set_feed_updates_seed_url,
    set_feed_updates_url,
    get_feed_updates_url,
    redis_set_metrics,
    set_feed_status,
)
from .app_utils import (
    Context,
    get_child_context,
    async_repeat_until_cancelled,
    cancel_non_current_tasks,
    sleep,
    http_429_retry_after,
    main,
)

EXCEPTION_INTERVALS = [1, 2, 4, 8, 16, 32, 64]
METRICS_INTERVAL = 1

UPDATES_INTERVAL = 1


async def run_outgoing_application():
    logger = get_root_logger('outgoing')

    with logged(logger, 'Examining environment', []):
        env = normalise_environment(os.environ)
        es_endpoint, redis_uri, sentry = get_common_config(env)
        feed_endpoints = [parse_feed_config(feed) for feed in env['FEEDS']]

    conn = aiohttp.TCPConnector(use_dns_cache=False, resolver=aiohttp.AsyncResolver())
    session = aiohttp.ClientSession(connector=conn, skip_auto_headers=['Accept-Encoding'])
    raven_client = get_raven_client(sentry, session)

    redis_client = await redis_get_client(redis_uri)

    metrics_registry = CollectorRegistry()
    metrics = get_metrics(metrics_registry)

    context = Context(
        logger=logger, metrics=metrics,
        raven_client=raven_client, redis_client=redis_client, session=session)

    await acquire_and_keep_lock(context, EXCEPTION_INTERVALS, 'lock')
    await create_outgoing_application(context, feed_endpoints, es_endpoint)
    await create_metrics_application(
        context, metrics_registry, feed_endpoints, es_endpoint,
    )

    async def cleanup():
        await cancel_non_current_tasks()

        redis_client.close()
        await redis_client.wait_closed()

        await session.close()
        # https://github.com/aio-libs/aiohttp/issues/1925
        await asyncio.sleep(0.250)

    return cleanup


async def create_outgoing_application(context, feed_endpoints, es_endpoint):
    async def ingester():
        await ingest_feeds(context, feed_endpoints, es_endpoint)

    asyncio.get_event_loop().create_task(
        async_repeat_until_cancelled(context, EXCEPTION_INTERVALS, ingester)
    )


async def ingest_feeds(context, feed_endpoints, es_endpoint):
    all_feed_ids = feed_unique_ids(feed_endpoints)
    indexes_without_alias, indexes_with_alias = await get_old_index_names(context, es_endpoint)

    indexes_to_delete = indexes_matching_no_feeds(
        indexes_without_alias + indexes_with_alias, all_feed_ids)
    await delete_indexes(
        get_child_context(context, 'initial-delete'), es_endpoint, indexes_to_delete,
    )

    def feed_ingester(ingest_type_context, feed_lock, feed_endpoint, ingest_func):
        async def _feed_ingester():
            await ingest_func(ingest_type_context, feed_lock, feed_endpoint, es_endpoint)
        return _feed_ingester

    await asyncio.gather(*[
        async_repeat_until_cancelled(context, feed_endpoint.exception_intervals, ingester)
        for feed_endpoint in feed_endpoints
        for feed_lock in [feed_endpoint.get_lock()]
        for feed_context in [get_child_context(context, feed_endpoint.unique_id)]
        for feed_func_ingest_type in [(ingest_feed_full, 'full'), (ingest_feed_updates, 'updates')]
        for ingest_type_logger in [get_child_context(feed_context, feed_func_ingest_type[1])]
        for ingester in [feed_ingester(ingest_type_logger, feed_lock, feed_endpoint,
                                       feed_func_ingest_type[0])]
    ])


def feed_unique_ids(feed_endpoints):
    return [feed_endpoint.unique_id for feed_endpoint in feed_endpoints]


async def ingest_feed_full(context, feed_lock, feed, es_endpoint):
    metrics = context.metrics
    with \
            logged(context.logger, 'Full ingest', []), \
            metric_timer(metrics['ingest_feed_duration_seconds'], [feed.unique_id, 'full']), \
            metric_inprogress(metrics['ingest_inprogress_ingests_total']):

        await set_feed_updates_seed_url_init(context, feed.unique_id)

        indexes_without_alias, _ = await get_old_index_names(context, es_endpoint)
        indexes_to_delete = indexes_matching_feeds(indexes_without_alias, [feed.unique_id])
        await delete_indexes(context, es_endpoint, indexes_to_delete)

        index_name = get_new_index_name(feed.unique_id)
        await create_index(context, es_endpoint, index_name)

        href = feed.seed
        while href:
            updates_href = href
            href = await ingest_feed_page(
                context, 'full', feed_lock, feed, es_endpoint, [index_name], href,
            )
            await sleep(context, feed.full_ingest_page_interval)

        await refresh_index(context, es_endpoint, index_name)
        await add_remove_aliases_atomically(context, es_endpoint, index_name, feed.unique_id)
        await set_feed_updates_seed_url(context, feed.unique_id, updates_href)


async def ingest_feed_updates(context, feed_lock, feed, es_endpoint):
    metrics = context.metrics
    with \
            logged(context.logger, 'Updates ingest', []), \
            metric_timer(metrics['ingest_feed_duration_seconds'], [feed.unique_id, 'updates']):

        href = await get_feed_updates_url(context, feed.unique_id)
        indexes_without_alias, indexes_with_alias = await get_old_index_names(context, es_endpoint)

        # We deliberatly ingest into both the live and ingesting indexes
        indexes_to_ingest_into = indexes_matching_feeds(
            indexes_without_alias + indexes_with_alias, [feed.unique_id])

        while href:
            updates_href = href
            href = await ingest_feed_page(context, 'updates', feed_lock, feed, es_endpoint,
                                          indexes_to_ingest_into, href)

        for index_name in indexes_matching_feeds(indexes_with_alias, [feed.unique_id]):
            await refresh_index(context, es_endpoint, index_name)
        await set_feed_updates_url(context, feed.unique_id, updates_href)

    await sleep(context, feed.updates_page_interval)


async def ingest_feed_page(context, ingest_type, feed_lock, feed, es_endpoint, index_names, href):
    with \
            logged(context.logger, 'Polling/pushing page', []), \
            metric_timer(context.metrics['ingest_page_duration_seconds'],
                         [feed.unique_id, ingest_type, 'total']):

        # Lock so there is only 1 request per feed at any given time
        async with feed_lock:
            with \
                    logged(context.logger, 'Polling page (%s)', [href]), \
                    metric_timer(context.metrics['ingest_page_duration_seconds'],
                                 [feed.unique_id, ingest_type, 'pull']):
                feed_contents = await get_feed_contents(context, href, feed.auth_headers(href),
                                                        _http_429_retry_after_context=context)

        with logged(context.logger, 'Parsing JSON', []):
            feed_parsed = ujson.loads(feed_contents)

        with logged(context.logger, 'Converting to bulk Elasticsearch items', []):
            es_bulk_items = feed.convert_to_bulk_es(feed_parsed, index_names)

        with \
                metric_timer(context.metrics['ingest_page_duration_seconds'],
                             [feed.unique_id, ingest_type, 'push']), \
                metric_counter(context.metrics['ingest_activities_nonunique_total'],
                               [feed.unique_id], len(es_bulk_items)):
            await es_bulk(context, es_endpoint, es_bulk_items)

        assumed_max_es_ingest_time = 10
        max_interval = \
            max(feed.full_ingest_page_interval, feed.updates_page_interval) + \
            assumed_max_es_ingest_time
        asyncio.ensure_future(set_feed_status(context, feed.unique_id, max_interval, b'GREEN'))

        return feed.next_href(feed_parsed)


@http_429_retry_after
async def get_feed_contents(context, href, headers, **_):
    async with context.session.get(href, headers=headers) as result:
        result.raise_for_status()
        return await result.read()


async def create_metrics_application(parent_context, metrics_registry, feed_endpoints,
                                     es_endpoint):
    context = get_child_context(parent_context, 'metrics')
    metrics = context.metrics

    async def poll_metrics():
        with logged(context.logger, 'Polling', []):
            searchable = await es_searchable_total(context, es_endpoint)
            metrics['elasticsearch_activities_total'].labels('searchable').set(searchable)

            await set_metric_if_can(
                metrics['elasticsearch_activities_total'],
                ['nonsearchable'],
                es_nonsearchable_total(context, es_endpoint),
            )
            await set_metric_if_can(
                metrics['elasticsearch_activities_age_minimum_seconds'],
                ['verification'],
                es_min_verification_age(context, es_endpoint),
            )

            feed_ids = feed_unique_ids(feed_endpoints)
            for feed_id in feed_ids:
                try:
                    searchable, nonsearchable = await es_feed_activities_total(
                        context, es_endpoint, feed_id)
                    metrics['elasticsearch_feed_activities_total'].labels(
                        feed_id, 'searchable').set(searchable)
                    metrics['elasticsearch_feed_activities_total'].labels(
                        feed_id, 'nonsearchable').set(nonsearchable)
                except ESMetricsUnavailable:
                    pass

        await redis_set_metrics(context, generate_latest(metrics_registry))
        await sleep(context, METRICS_INTERVAL)

    asyncio.get_event_loop().create_task(
        async_repeat_until_cancelled(context, EXCEPTION_INTERVALS, poll_metrics)
    )


async def set_metric_if_can(metric, labels, get_value_coroutine):
    try:
        metric.labels(*labels).set(await get_value_coroutine)
    except ESMetricsUnavailable:
        pass


if __name__ == '__main__':
    main(run_outgoing_application)
