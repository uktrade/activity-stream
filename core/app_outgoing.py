import asyncio
import json
import logging
import os

import aiohttp
import aioredis
from prometheus_client import (
    CollectorRegistry,
    generate_latest,
)

from .app_elasticsearch import (
    ESMetricsUnavailable,
    es_bulk,
    es_feed_activities_total,
    es_searchable_total,
    es_nonsearchable_total,
    es_min_verification_age,
    create_index,
    create_mapping,
    get_new_index_name,
    get_old_index_names,
    indexes_matching_feeds,
    indexes_matching_no_feeds,
    add_remove_aliases_atomically,
    delete_indexes,
    refresh_index,
)

from .app_feeds import (
    ActivityStreamFeed,
    ZendeskFeed,
)
from .app_metrics import (
    async_inprogress,
    async_timer,
    get_metrics,
)
from .app_utils import (
    normalise_environment,
    get_common_config,
    get_raven_client,
    async_repeat_until_cancelled,
    cancel_non_current_tasks,
    main,
)

EXCEPTION_INTERVAL = 60
METRICS_INTERVAL = 1


async def run_outgoing_application():
    app_logger = logging.getLogger('activity-stream')

    app_logger.debug('Examining environment...')
    env = normalise_environment(os.environ)

    es_endpoint, redis_uri, sentry = get_common_config(env)
    feed_endpoints = [parse_feed_config(feed) for feed in env['FEEDS']]

    app_logger.debug('Examining environment: done')

    raven_client = get_raven_client(sentry)
    session = aiohttp.ClientSession(skip_auto_headers=['Accept-Encoding'])
    redis_client = await aioredis.create_redis(redis_uri)

    metrics_registry = CollectorRegistry()
    metrics = get_metrics(metrics_registry)
    await create_outgoing_application(
        metrics, raven_client, session, feed_endpoints, es_endpoint,
    )
    await create_metrics_application(
        metrics, metrics_registry, redis_client, raven_client,
        session, feed_endpoints, es_endpoint,
    )

    async def cleanup():
        await cancel_non_current_tasks()
        await raven_client.remote.get_transport().close()

        await session.close()
        # https://github.com/aio-libs/aiohttp/issues/1925
        await asyncio.sleep(0.250)

    return cleanup


async def create_outgoing_application(metrics, raven_client, session, feed_endpoints, es_endpoint):
    asyncio.get_event_loop().create_task(ingest_feeds(
        metrics, raven_client, session, feed_endpoints, es_endpoint,
        _async_repeat_until_cancelled_raven_client=raven_client,
        _async_repeat_until_cancelled_exception_interval=EXCEPTION_INTERVAL,
        _async_repeat_until_cancelled_logging_title='Polling feeds',
    ))


@async_repeat_until_cancelled
async def ingest_feeds(metrics, raven_client, session, feed_endpoints, es_endpoint, **_):
    all_feed_ids = feed_unique_ids(feed_endpoints)
    indexes_without_alias, indexes_with_alias = await get_old_index_names(session, es_endpoint)

    indexes_to_delete = indexes_matching_no_feeds(
        indexes_without_alias + indexes_with_alias, all_feed_ids)
    await delete_indexes(session, es_endpoint, indexes_to_delete)

    await asyncio.gather(*[
        ingest_feed(
            metrics, session, feed_endpoint, es_endpoint,
            _async_repeat_until_cancelled_raven_client=raven_client,
            _async_repeat_until_cancelled_exception_interval=EXCEPTION_INTERVAL,
            _async_repeat_until_cancelled_logging_title='Polling feed',
            _async_timer=metrics['ingest_feed_duration_seconds'],
            _async_timer_labels=[feed_endpoint.unique_id],
            _async_inprogress=metrics['ingest_inprogress_ingests_total'],
        )
        for feed_endpoint in feed_endpoints
    ])


def feed_unique_ids(feed_endpoints):
    return [feed_endpoint.unique_id for feed_endpoint in feed_endpoints]


@async_repeat_until_cancelled
@async_inprogress
@async_timer
async def ingest_feed(metrics, session, feed, es_endpoint, **_):
    app_logger = logging.getLogger('activity-stream')

    indexes_without_alias, _ = await get_old_index_names(session, es_endpoint)
    indexes_to_delete = indexes_matching_feeds(indexes_without_alias, [feed.unique_id])
    await delete_indexes(session, es_endpoint, indexes_to_delete)

    index_name = get_new_index_name(feed.unique_id)
    await create_index(session, es_endpoint, index_name)
    await create_mapping(session, es_endpoint, index_name)

    href = feed.seed
    while href:
        href, interval, message = await ingest_feed_page(
            metrics, session, feed, es_endpoint, index_name, href,
            _async_timer=metrics['ingest_page_duration_seconds'],
            _async_timer_labels=[feed.unique_id, 'total'],
        )
        app_logger.debug(message)
        app_logger.debug('Sleeping for %s seconds', interval)

        await asyncio.sleep(interval)

    await refresh_index(session, es_endpoint, index_name)
    await add_remove_aliases_atomically(session, es_endpoint, index_name, feed.unique_id)


@async_timer
async def ingest_feed_page(metrics, session, feed, es_endpoint, index_name, href, **_):
    app_logger = logging.getLogger('activity-stream')

    app_logger.debug('Polling')
    feed_contents = await get_feed_contents(
        session, href, feed.auth_headers(href),
        _async_timer=metrics['ingest_page_duration_seconds'],
        _async_timer_labels=[feed.unique_id, 'pull'],
    )

    app_logger.debug('Parsing JSON...')
    feed_parsed = json.loads(feed_contents)
    app_logger.debug('Parsed')

    es_bulk_items = feed.convert_to_bulk_es(feed_parsed, index_name)
    await es_bulk(
        session, es_endpoint, es_bulk_items,
        _async_timer=metrics['ingest_page_duration_seconds'],
        _async_timer_labels=[feed.unique_id, 'push'],
        _async_counter=metrics['ingest_activities_nonunique_total'],
        _async_counter_labels=[feed.unique_id],
        _async_counter_increment_by=len(es_bulk_items),
    )

    app_logger.debug('Finding next URL...')
    next_href = feed.next_href(feed_parsed)
    app_logger.debug('Finding next URL: done (%s)', next_href)

    interval, message = \
        (feed.polling_page_interval, 'Will poll next page in feed') if next_href else \
        (feed.polling_seed_interval, 'Will poll seed page')

    return next_href, interval, message


@async_timer
async def get_feed_contents(session, href, headers, **_):
    app_logger = logging.getLogger('activity-stream')

    app_logger.debug('Fetching feed...')
    result = await session.get(href, headers=headers)
    app_logger.debug('Fetching feed: done')

    if result.status != 200:
        raise Exception(await result.text())

    app_logger.debug('Fetching feed contents...')
    contents = await result.read()
    app_logger.debug('Fetched feed contents: done')

    return contents


def parse_feed_config(feed_config):
    by_feed_type = {
        'activity_stream': ActivityStreamFeed,
        'zendesk': ZendeskFeed,
    }
    return by_feed_type[feed_config['TYPE']].parse_config(feed_config)


async def create_metrics_application(metrics, metrics_registry, redis_client,
                                     raven_client, session, feed_endpoints, es_endpoint):

    @async_repeat_until_cancelled
    async def poll_metrics(**_):
        searchable = await es_searchable_total(session, es_endpoint)
        metrics['elasticsearch_activities_total'].labels('searchable').set(searchable)

        try:
            nonsearchable = await es_nonsearchable_total(session, es_endpoint)
            metrics['elasticsearch_activities_total'].labels('nonsearchable').set(nonsearchable)
        except ESMetricsUnavailable:
            pass

        try:
            min_activity_age = await es_min_verification_age(session, es_endpoint)
            metrics['elasticsearch_activities_age_minimum_seconds'].labels(
                'verification').set(min_activity_age)
        except ESMetricsUnavailable:
            pass

        feed_ids = feed_unique_ids(feed_endpoints)
        for feed_id in feed_ids:
            try:
                searchable, nonsearchable = await es_feed_activities_total(session,
                                                                           es_endpoint, feed_id)
                metrics['elasticsearch_feed_activities_total'].labels(
                    feed_id, 'searchable').set(searchable)
                metrics['elasticsearch_feed_activities_total'].labels(
                    feed_id, 'nonsearchable').set(nonsearchable)
            except ESMetricsUnavailable:
                pass

        await redis_client.set('metrics', generate_latest(metrics_registry))
        await asyncio.sleep(METRICS_INTERVAL)

    asyncio.get_event_loop().create_task(poll_metrics(
        _async_repeat_until_cancelled_raven_client=raven_client,
        _async_repeat_until_cancelled_exception_interval=METRICS_INTERVAL,
        _async_repeat_until_cancelled_logging_title='Elasticsearch polling',
    ))


if __name__ == '__main__':
    main(run_outgoing_application)
