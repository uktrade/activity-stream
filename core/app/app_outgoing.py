import asyncio
import os

import aiohttp
from prometheus_client import (
    CollectorRegistry,
    generate_latest,
)
import ujson

from .app_outgoing_elasticsearch import (
    ESMetricsUnavailable,
    es_bulk,
    es_feed_activities_total,
    es_searchable_total,
    es_nonsearchable_total,
    create_activities_index,
    create_objects_index,
    get_new_index_names,
    get_old_index_names,
    split_index_names,
    indexes_matching_feeds,
    indexes_matching_no_feeds,
    add_remove_aliases_atomically,
    delete_indexes,
    refresh_index,
)
from .elasticsearch import (
    es_min_verification_age,
)

from .feeds import (
    parse_feed_config,
)
from .http import (
    http_make_request,
)
from .logger import (
    get_root_logger,
    logged,
)
from .metrics import (
    metric_counter,
    metric_inprogress,
    metric_timer,
    get_metrics,
)
from .raven import (
    get_raven_client,
)
from .redis import (
    redis_get_client,
)
from .app_outgoing_redis import (
    acquire_and_keep_lock,
    set_feed_updates_seed_url_init,
    set_feed_updates_seed_url,
    set_feed_updates_url,
    get_feed_updates_url,
    redis_set_metrics,
    set_feed_status,
)
from .app_outgoing_utils import (
    repeat_until_cancelled,
)
from .utils import (
    Context,
    cancel_non_current_tasks,
    get_child_context,
    get_common_config,
    main,
    normalise_environment,
    sleep,
)
from . import settings

EXCEPTION_INTERVALS = [1, 2, 4, 8, 16, 32, 64]
METRICS_INTERVAL = 1

UPDATES_INTERVAL = 1


async def run_outgoing_application():
    logger = get_root_logger('outgoing')

    with logged(logger, 'Examining environment', []):
        env = normalise_environment(os.environ)
        es_uri, redis_uri, sentry = get_common_config(env)
        feed_endpoints = [parse_feed_config(feed) for feed in env['FEEDS']]

    settings.ES_URI = es_uri
    conn = aiohttp.TCPConnector(use_dns_cache=False, resolver=aiohttp.AsyncResolver())
    session = aiohttp.ClientSession(
        connector=conn,
        headers={'Accept-Encoding': 'identity;q=1.0, *;q=0'},
    )
    redis_client = await redis_get_client(redis_uri)

    metrics_registry = CollectorRegistry()
    metrics = get_metrics(metrics_registry)
    raven_client = get_raven_client(sentry, session, metrics)

    context = Context(
        logger=logger, metrics=metrics,
        raven_client=raven_client, redis_client=redis_client, session=session)

    await acquire_and_keep_lock(context, EXCEPTION_INTERVALS, 'lock')
    await create_outgoing_application(context, feed_endpoints)
    await create_metrics_application(
        context, metrics_registry, feed_endpoints,
    )

    async def cleanup():
        await cancel_non_current_tasks()

        redis_client.close()
        await redis_client.wait_closed()

        await session.close()
        # https://github.com/aio-libs/aiohttp/issues/1925
        await asyncio.sleep(0.250)

    return cleanup


async def create_outgoing_application(context, feed_endpoints):
    asyncio.get_event_loop().create_task(
        repeat_until_cancelled(
            context, EXCEPTION_INTERVALS,
            to_repeat=ingest_feeds, to_repeat_args=(context, feed_endpoints),
        )
    )


async def ingest_feeds(context, feed_endpoints):
    all_feed_ids = feed_unique_ids(feed_endpoints)
    indexes_without_alias, indexes_with_alias = await get_old_index_names(context)

    indexes_to_delete = indexes_matching_no_feeds(
        indexes_without_alias + indexes_with_alias, all_feed_ids)
    await delete_indexes(
        get_child_context(context, 'initial-delete'), indexes_to_delete,
    )

    await asyncio.gather(*[
        repeat_until_cancelled(
            context, feed_endpoint.exception_intervals,
            to_repeat=ingest_func, to_repeat_args=(ingest_context, feed_endpoint),
        )
        for feed_endpoint in feed_endpoints
        for (ingest_func, ingest_context) in [
            (ingest_full, get_child_context(context, f'{feed_endpoint.unique_id},full')),
            (ingest_updates, get_child_context(context, f'{feed_endpoint.unique_id},updates')),
        ]
    ])


def feed_unique_ids(feed_endpoints):
    return [feed_endpoint.unique_id for feed_endpoint in feed_endpoints]


async def ingest_full(context, feed):
    metrics = context.metrics
    with \
            logged(context.logger, 'Full ingest', []), \
            metric_timer(metrics['ingest_feed_duration_seconds'], [feed.unique_id, 'full']), \
            metric_inprogress(metrics['ingest_inprogress_ingests_total']):

        await set_feed_updates_seed_url_init(context, feed.unique_id)

        indexes_without_alias, _ = await get_old_index_names(context)
        indexes_to_delete = indexes_matching_feeds(indexes_without_alias, [feed.unique_id])
        await delete_indexes(context, indexes_to_delete)

        activities_index_name, objects_index_name = get_new_index_names(feed.unique_id)
        await create_activities_index(context, activities_index_name)
        await create_objects_index(context, objects_index_name)

        href = feed.seed
        while href:
            updates_href = href
            href = await ingest_page(
                context, 'full', feed, [activities_index_name], [objects_index_name], href,
            )
            await sleep(context, feed.full_ingest_page_interval)

        await refresh_index(context, activities_index_name)
        await refresh_index(context, objects_index_name)
        await add_remove_aliases_atomically(
            context, activities_index_name, objects_index_name, feed.unique_id)
        await set_feed_updates_seed_url(context, feed.unique_id, updates_href)


async def ingest_updates(context, feed):
    metrics = context.metrics
    with \
            logged(context.logger, 'Updates ingest', []), \
            metric_timer(metrics['ingest_feed_duration_seconds'], [feed.unique_id, 'updates']):

        href = await get_feed_updates_url(context, feed.unique_id)
        indexes_without_alias, indexes_with_alias = await get_old_index_names(context)

        # We deliberatly ingest into both the live and ingesting indexes
        indexes_to_ingest_into = indexes_matching_feeds(
            indexes_without_alias + indexes_with_alias, [feed.unique_id])

        activities_index_names, objects_index_names = split_index_names(indexes_to_ingest_into)

        while href:
            updates_href = href
            href = await ingest_page(
                context, 'updates', feed, activities_index_names, objects_index_names, href,
            )

        for index_name in indexes_matching_feeds(indexes_with_alias, [feed.unique_id]):
            await refresh_index(context, index_name)
        await set_feed_updates_url(context, feed.unique_id, updates_href)

    await sleep(context, feed.updates_page_interval)


async def ingest_page(context, ingest_type, feed, activity_index_names, objects_index_names, href):
    with \
            logged(context.logger, 'Polling/pushing page', []), \
            metric_timer(context.metrics['ingest_page_duration_seconds'],
                         [feed.unique_id, ingest_type, 'total']):

        # Lock so there is only 1 request per feed at any given time
        async with feed.lock:
            with \
                    logged(context.logger, 'Polling page (%s)', [href]), \
                    metric_timer(context.metrics['ingest_page_duration_seconds'],
                                 [feed.unique_id, ingest_type, 'pull']):
                feed_contents = await get_feed_contents(
                    context, href, await feed.auth_headers(context, href),
                )

        with logged(context.logger, 'Parsing JSON', []):
            feed_parsed = ujson.loads(feed_contents)

        with logged(context.logger, 'Converting to bulk Elasticsearch items', []):
            es_bulk_items = await feed.convert_to_bulk_es(
                context, feed_parsed, activity_index_names, objects_index_names)

        with \
                metric_timer(context.metrics['ingest_page_duration_seconds'],
                             [feed.unique_id, ingest_type, 'push']), \
                metric_counter(context.metrics['ingest_activities_nonunique_total'],
                               [feed.unique_id], len(es_bulk_items)):
            await es_bulk(context, es_bulk_items)

        asyncio.ensure_future(set_feed_status(
            context, feed.unique_id, feed.max_interval_before_reporting_down, b'GREEN'))

        return feed.next_href(feed_parsed)


async def get_feed_contents(context, href, headers):
    num_attempts = 0
    max_attempts = 10
    logger = context.logger

    while True:
        num_attempts += 1
        try:
            result = await http_make_request(
                context.session, context.metrics, 'GET', href, data=b'', headers=headers)
            result.raise_for_status()
            return result._body
        except aiohttp.ClientResponseError as client_error:
            if (num_attempts >= max_attempts or client_error.status != 429 or
                    'Retry-After' not in client_error.headers):
                raise
            logger.debug('HTTP 429 received at attempt (%s). Will retry after (%s) seconds',
                         num_attempts, client_error.headers['Retry-After'])
            await sleep(context, int(client_error.headers['Retry-After']))


async def create_metrics_application(parent_context, metrics_registry, feed_endpoints):
    context = get_child_context(parent_context, 'metrics')
    metrics = context.metrics

    async def poll_metrics():
        with logged(context.logger, 'Polling', []):
            searchable = await es_searchable_total(context)
            metrics['elasticsearch_activities_total'].labels('searchable').set(searchable)

            await set_metric_if_can(
                metrics['elasticsearch_activities_total'],
                ['nonsearchable'],
                es_nonsearchable_total(context),
            )
            await set_metric_if_can(
                metrics['elasticsearch_activities_age_minimum_seconds'],
                ['verification'],
                es_min_verification_age(context),
            )

            feed_ids = feed_unique_ids(feed_endpoints)
            for feed_id in feed_ids:
                try:
                    searchable, nonsearchable = await es_feed_activities_total(
                        context, feed_id)
                    metrics['elasticsearch_feed_activities_total'].labels(
                        feed_id, 'searchable').set(searchable)
                    metrics['elasticsearch_feed_activities_total'].labels(
                        feed_id, 'nonsearchable').set(nonsearchable)
                except ESMetricsUnavailable:
                    pass

        await redis_set_metrics(context, generate_latest(metrics_registry))
        await sleep(context, METRICS_INTERVAL)

    asyncio.get_event_loop().create_task(
        repeat_until_cancelled(context, EXCEPTION_INTERVALS, to_repeat=poll_metrics)
    )


async def set_metric_if_can(metric, labels, get_value_coroutine):
    try:
        metric.labels(*labels).set(await get_value_coroutine)
    except ESMetricsUnavailable:
        pass


if __name__ == '__main__':
    main(run_outgoing_application)
