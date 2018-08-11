import asyncio
import json
import os

import aiohttp
import aioredis
from prometheus_client import (
    CollectorRegistry,
    generate_latest,
)

from shared.logger import (
    get_root_logger,
    get_child_logger,
    async_logger,
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
    get_raven_client,
    async_repeat_until_cancelled,
    cancel_non_current_tasks,
    main,
)

EXCEPTION_INTERVAL = 60
METRICS_INTERVAL = 1


async def run_outgoing_application():
    logger = get_root_logger('outgoing')

    with logged(logger, 'Examining environment', []):
        env = normalise_environment(os.environ)
        es_endpoint, redis_uri, sentry = get_common_config(env)
        feed_endpoints = [parse_feed_config(feed) for feed in env['FEEDS']]

    raven_client = get_raven_client(sentry)
    session = aiohttp.ClientSession(skip_auto_headers=['Accept-Encoding'])
    redis_client = await aioredis.create_redis(redis_uri)

    metrics_registry = CollectorRegistry()
    metrics = get_metrics(metrics_registry)

    await acquire_and_keep_lock(logger, redis_client, raven_client)

    await create_outgoing_application(
        logger, metrics, raven_client, session, feed_endpoints, es_endpoint,
    )
    await create_metrics_application(
        logger, metrics, metrics_registry, redis_client, raven_client,
        session, feed_endpoints, es_endpoint,
    )

    async def cleanup():
        await cancel_non_current_tasks()
        await raven_client.remote.get_transport().close()

        await session.close()
        # https://github.com/aio-libs/aiohttp/issues/1925
        await asyncio.sleep(0.250)

    return cleanup


async def acquire_and_keep_lock(parent_logger, redis_client, raven_client):
    ''' Prevents Elasticsearch errors during deployments

    The exceptions would be caused by a new deployment deleting indexes while
    the previous deployment is still ingesting into them

    We do not offer a delete for simplicity: the lock will just expire if it's
    not extended, which happens on destroy of the application

    We don't use Redlock, since we don't care too much if a Redis failure causes
    multiple clients to have the lock for a period of time. It would only cause
    Elasticsearch errors to appear in sentry, but otherwise there would be no
    harm

    We don't try to re-aquire the lock if we've lost it. This would happen if
    we've blocked for > ttl and lost the lock. We _want_ to have more evidence
    of this so we can address the problem.
    '''
    logger = get_child_logger(parent_logger, 'lock')
    ttl = 2
    aquire_interval = 1
    extend_interval = 1
    key = 'lock'

    async def acquire():
        while True:
            logger.debug('Acquiring...')
            response = await redis_client.execute('SET', key, '1', 'EX', ttl, 'NX')
            if response == b'OK':
                logger.debug('Acquiring... (done)')
                break
            logger.debug('Acquiring... (failed)')
            await sleep(
                aquire_interval,
                _async_logger=logger,
                _async_logger_args=[aquire_interval],
            )

    @async_repeat_until_cancelled
    async def extend_forever(**_):
        await asyncio.sleep(extend_interval)
        response = await redis_client.execute('EXPIRE', key, ttl)
        if response != 1:
            raise Exception('Lock has been lost')

    await acquire()
    asyncio.get_event_loop().create_task(extend_forever(
        _async_repeat_until_cancelled_raven_client=raven_client,
        _async_repeat_until_cancelled_exception_interval=extend_interval,
        _async_repeat_until_cancelled_logger=logger,
    ))


async def create_outgoing_application(logger, metrics, raven_client, session,
                                      feed_endpoints, es_endpoint):
    asyncio.get_event_loop().create_task(ingest_feeds(
        logger, metrics, raven_client, session, feed_endpoints, es_endpoint,
        _async_repeat_until_cancelled_raven_client=raven_client,
        _async_repeat_until_cancelled_exception_interval=EXCEPTION_INTERVAL,
        _async_repeat_until_cancelled_logger=logger,
    ))


@async_repeat_until_cancelled
async def ingest_feeds(logger, metrics, raven_client, session, feed_endpoints, es_endpoint, **_):
    all_feed_ids = feed_unique_ids(feed_endpoints)
    indexes_without_alias, indexes_with_alias = await get_old_index_names(
        logger, session, es_endpoint,
    )

    indexes_to_delete = indexes_matching_no_feeds(
        indexes_without_alias + indexes_with_alias, all_feed_ids)
    await delete_indexes(
        logger, session, es_endpoint, indexes_to_delete,
    )

    await asyncio.gather(*[
        ingest_feed(
            feed_logger, metrics, session, feed_endpoint, es_endpoint,
            _async_repeat_until_cancelled_raven_client=raven_client,
            _async_repeat_until_cancelled_exception_interval=EXCEPTION_INTERVAL,
            _async_repeat_until_cancelled_logger=logger,
            _async_timer=metrics['ingest_feed_duration_seconds'],
            _async_timer_labels=[feed_endpoint.unique_id],
            _async_inprogress=metrics['ingest_inprogress_ingests_total'],
            _async_logger=feed_logger,
            _async_logger_args=[],
        )
        for feed_endpoint in feed_endpoints
        for feed_logger in [get_child_logger(logger, feed_endpoint.unique_id)]
    ])


def feed_unique_ids(feed_endpoints):
    return [feed_endpoint.unique_id for feed_endpoint in feed_endpoints]


@async_repeat_until_cancelled
@async_logger('Full ingest')
@async_inprogress
@async_timer
async def ingest_feed(logger, metrics, session, feed, es_endpoint, **_):
    indexes_without_alias, _ = await get_old_index_names(
        logger, session, es_endpoint,
    )
    indexes_to_delete = indexes_matching_feeds(indexes_without_alias, [feed.unique_id])
    await delete_indexes(logger, session, es_endpoint, indexes_to_delete)

    index_name = get_new_index_name(feed.unique_id)
    await create_index(logger, session, es_endpoint, index_name)
    await create_mapping(logger, session, es_endpoint, index_name)

    href = feed.seed
    while href:
        href, interval = await ingest_feed_page(
            logger, metrics, session, feed, es_endpoint, index_name, href,
            _async_timer=metrics['ingest_page_duration_seconds'],
            _async_timer_labels=[feed.unique_id, 'total'],
            _async_logger=logger,
            _async_logger_args=[],
        )
        await sleep(
            interval,
            _async_logger=logger,
            _async_logger_args=[interval],
        )

    await refresh_index(logger, session, es_endpoint, index_name)

    await add_remove_aliases_atomically(
        logger, session, es_endpoint, index_name, feed.unique_id,
    )


@async_logger('Sleeping for %s seconds')
async def sleep(interval, **_):
    await asyncio.sleep(interval)


@async_logger('Polling/pushing page')
@async_timer
async def ingest_feed_page(logger, metrics, session, feed, es_endpoint, index_name, href, **_):
    feed_contents = await get_feed_contents(
        session, href, feed.auth_headers(href),
        _async_timer=metrics['ingest_page_duration_seconds'],
        _async_timer_labels=[feed.unique_id, 'pull'],
        _async_logger=logger,
        _async_logger_args=[href],
    )

    with logged(logger, 'Parsing JSON', []):
        feed_parsed = json.loads(feed_contents)

    with logged(logger, 'Converting to bulk Elasticsearch items', []):
        es_bulk_items = feed.convert_to_bulk_es(feed_parsed, index_name)

    await es_bulk(
        logger, session, es_endpoint, es_bulk_items,
        _async_timer=metrics['ingest_page_duration_seconds'],
        _async_timer_labels=[feed.unique_id, 'push'],
        _async_counter=metrics['ingest_activities_nonunique_total'],
        _async_counter_labels=[feed.unique_id],
        _async_counter_increment_by=len(es_bulk_items),
    )

    next_href = feed.next_href(feed_parsed)

    interval = \
        feed.polling_page_interval if next_href else \
        feed.polling_seed_interval

    return next_href, interval


@async_logger('Polling feed (%s)')
@async_timer
async def get_feed_contents(session, href, headers, **_):
    result = await session.get(href, headers=headers)
    if result.status != 200:
        raise Exception(await result.text())

    contents = await result.read()

    return contents


def parse_feed_config(feed_config):
    by_feed_type = {
        'activity_stream': ActivityStreamFeed,
        'zendesk': ZendeskFeed,
    }
    return by_feed_type[feed_config['TYPE']].parse_config(feed_config)


async def create_metrics_application(logger, metrics, metrics_registry, redis_client,
                                     raven_client, session, feed_endpoints, es_endpoint):
    logger = get_child_logger(logger, 'metrics')

    @async_repeat_until_cancelled
    @async_logger('Polling')
    async def poll_metrics(**_):
        searchable = await es_searchable_total(logger, session, es_endpoint)
        metrics['elasticsearch_activities_total'].labels('searchable').set(searchable)

        await set_metric_if_can(
            metrics['elasticsearch_activities_total'],
            ['nonsearchable'],
            es_nonsearchable_total(logger, session, es_endpoint),
        )
        await set_metric_if_can(
            metrics['elasticsearch_activities_age_minimum_seconds'],
            ['verification'],
            es_min_verification_age(logger, session, es_endpoint),
        )

        feed_ids = feed_unique_ids(feed_endpoints)
        for feed_id in feed_ids:
            try:
                searchable, nonsearchable = await es_feed_activities_total(logger, session,
                                                                           es_endpoint, feed_id)
                metrics['elasticsearch_feed_activities_total'].labels(
                    feed_id, 'searchable').set(searchable)
                metrics['elasticsearch_feed_activities_total'].labels(
                    feed_id, 'nonsearchable').set(nonsearchable)
            except ESMetricsUnavailable:
                pass

        await save_metrics_to_redis(
            generate_latest(metrics_registry),
            _async_logger=logger,
            _async_logger_args=[],
        )

        await sleep(
            METRICS_INTERVAL,
            _async_logger=logger,
            _async_logger_args=[METRICS_INTERVAL]
        )

    @async_logger('Saving to Redis')
    async def save_metrics_to_redis(metrics, **_):
        await redis_client.set('metrics', metrics)

    asyncio.get_event_loop().create_task(poll_metrics(
        _async_repeat_until_cancelled_raven_client=raven_client,
        _async_repeat_until_cancelled_exception_interval=METRICS_INTERVAL,
        _async_repeat_until_cancelled_logger=logger,
        _async_logger=logger,
        _async_logger_args=[],
    ))


async def set_metric_if_can(metric, labels, get_value_coroutine):
    try:
        metric.labels(*labels).set(await get_value_coroutine)
    except ESMetricsUnavailable:
        pass


if __name__ == '__main__':
    main(run_outgoing_application)
