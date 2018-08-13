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
    ActivityStreamFeed,
    ZendeskFeed,
)
from .app_metrics import (
    metric_counter,
    metric_inprogress,
    metric_timer,
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

# So the latest URL for feeds don't hang around in Redis for
# ever if the feed is turned off
FEED_UPDATE_URL_EXPIRE = 60 * 60 * 24 * 31
NOT_EXISTS = b'__NOT_EXISTS__'


UPDATES_INTERVAL = 1


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
        logger, metrics, raven_client, redis_client, session, feed_endpoints, es_endpoint,
    )

    redis_client = await aioredis.create_redis(redis_uri)
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
            await sleep(logger, aquire_interval)

    async def extend_forever():
        await sleep(logger, extend_interval)
        response = await redis_client.execute('EXPIRE', key, ttl)
        if response != 1:
            raven_client.captureMessage('Lock has been lost')
            await acquire()

    await acquire()
    asyncio.get_event_loop().create_task(async_repeat_until_cancelled(
        logger, raven_client, extend_interval,
        extend_forever,
    ))


async def create_outgoing_application(logger, metrics, raven_client, redis_client, session,
                                      feed_endpoints, es_endpoint):
    async def ingester():
        await ingest_feeds(
            logger, metrics, raven_client, redis_client, session, feed_endpoints, es_endpoint,
        )
    asyncio.get_event_loop().create_task(
        async_repeat_until_cancelled(logger, raven_client, EXCEPTION_INTERVAL, ingester)
    )


async def ingest_feeds(logger, metrics, raven_client, redis_client, session,
                       feed_endpoints, es_endpoint):
    all_feed_ids = feed_unique_ids(feed_endpoints)
    indexes_without_alias, indexes_with_alias = await get_old_index_names(
        logger, session, es_endpoint,
    )

    indexes_to_delete = indexes_matching_no_feeds(
        indexes_without_alias + indexes_with_alias, all_feed_ids)
    await delete_indexes(
        logger, session, es_endpoint, indexes_to_delete,
    )

    def feed_ingester(ingest_type_logger, feed_lock, feed_endpoint, ingest_func):
        async def _feed_ingester():
            await ingest_func(ingest_type_logger, metrics, redis_client, session, feed_lock,
                              feed_endpoint, es_endpoint)
        return _feed_ingester

    await asyncio.gather(*[
        async_repeat_until_cancelled(
            ingest_type_logger, raven_client, EXCEPTION_INTERVAL, ingester,
        )
        for feed_endpoint in feed_endpoints
        for feed_lock in [asyncio.Lock()]
        for feed_logger in [get_child_logger(logger, feed_endpoint.unique_id)]
        for feed_func_ingest_type in [(ingest_feed_full, 'full'), (ingest_feed_updates, 'updates')]
        for ingest_type_logger in [get_child_logger(feed_logger, feed_func_ingest_type[1])]
        for ingester in [feed_ingester(ingest_type_logger, feed_lock, feed_endpoint,
                                       feed_func_ingest_type[0])]
    ])


def feed_unique_ids(feed_endpoints):
    return [feed_endpoint.unique_id for feed_endpoint in feed_endpoints]


async def ingest_feed_full(logger, metrics, redis_client, session, feed_lock, feed, es_endpoint):
    with \
            logged(logger, 'Full ingest', []), \
            metric_timer(metrics['ingest_feed_duration_seconds'], [feed.unique_id, 'full']), \
            metric_inprogress(metrics['ingest_inprogress_ingests_total']):

        await set_feed_updates_seed_url_init(redis_client, feed)
        indexes_without_alias, _ = await get_old_index_names(
            logger, session, es_endpoint,
        )
        indexes_to_delete = indexes_matching_feeds(indexes_without_alias, [feed.unique_id])
        await delete_indexes(logger, session, es_endpoint, indexes_to_delete)

        index_name = get_new_index_name(feed.unique_id)
        await create_index(logger, session, es_endpoint, index_name)

        href = feed.seed
        while href:
            updates_href = href
            href = await ingest_feed_page(
                logger, metrics, session, 'full', feed_lock, feed, es_endpoint, [index_name], href
            )
            await sleep(logger, feed.polling_page_interval)

        await refresh_index(logger, session, es_endpoint, index_name)

        await add_remove_aliases_atomically(
            logger, session, es_endpoint, index_name, feed.unique_id,
        )

        await set_feed_updates_seed_url(logger, redis_client, feed, updates_href)


async def ingest_feed_updates(logger, metrics, redis_client, session, feed_lock, feed,
                              es_endpoint):
    with \
            logged(logger, 'Updates ingest', []), \
            metric_timer(metrics['ingest_feed_duration_seconds'], [feed.unique_id, 'updates']):

        href = await get_feed_updates_url(logger, redis_client, feed)
        indexes_without_alias, indexes_with_alias = await get_old_index_names(
            logger, session, es_endpoint,
        )

        # We deliberatly ingest into both the live and ingesting indexes
        indexes_to_ingest_into = indexes_matching_feeds(
            indexes_without_alias + indexes_with_alias, [feed.unique_id])

        while href:
            updates_href = href
            href = await ingest_feed_page(logger, metrics, session, 'updates', feed_lock, feed,
                                          es_endpoint, indexes_to_ingest_into, href)

        for index_name in indexes_matching_feeds(indexes_with_alias, [feed.unique_id]):
            await refresh_index(logger, session, es_endpoint, index_name)
        await set_feed_updates_url(logger, redis_client, feed, updates_href)

    await sleep(logger, UPDATES_INTERVAL)


async def set_feed_updates_seed_url_init(redis_client, feed):
    updates_seed_url_key = 'feed-updates-seed-url-' + feed.unique_id
    await redis_client.execute('SET', updates_seed_url_key, NOT_EXISTS,
                               'EX', FEED_UPDATE_URL_EXPIRE,
                               'NX')


async def set_feed_updates_seed_url(logger, redis_client, feed, updates_url):
    updates_seed_url_key = 'feed-updates-seed-url-' + feed.unique_id
    with logged(logger, 'Setting updates seed url to (%s)', [updates_url]):
        await redis_client.execute('SET', updates_seed_url_key, updates_url,
                                   'EX', FEED_UPDATE_URL_EXPIRE)


async def set_feed_updates_url(logger, redis_client, feed, updates_url):
    updates_latest_url_key = 'feed-updates-latest-url-' + feed.unique_id
    with logged(logger, 'Setting updates url to (%s)', [updates_url]):
        await redis_client.execute('SET', updates_latest_url_key, updates_url,
                                   'EX', FEED_UPDATE_URL_EXPIRE)


async def get_feed_updates_url(logger, redis_client, feed):
    # For each live update, if a full ingest has recently finished, we use the URL set
    # by that (required, for example, if the endpoint has changed, or if lots of recent
    # event have been deleted). Otherwise, we use the URL set by the latest updates pass

    updates_seed_url_key = 'feed-updates-seed-url-' + feed.unique_id
    updates_latest_url_key = 'feed-updates-latest-url-' + feed.unique_id
    with logged(logger, 'Getting updates url', []):
        while True:
            # We want the equivalent of an atomic GET/DEL, to avoid the race condition that the
            # full ingest sets the updates seed URL, but the updates chain then overwrites it
            # There is no atomic GETDEL command available, but it could be done with redis multi
            # exec, but, we have multiple concurrent usages of the redis client, which I suspect
            # would make transactions impossible right now
            # We do have an atomic GETSET however, so we use that with a special NOT_EXISTS value
            updates_seed_url = await redis_client.execute('GETSET', updates_seed_url_key,
                                                          NOT_EXISTS)
            if updates_seed_url is not None and updates_seed_url != NOT_EXISTS:
                url = updates_seed_url
                break

            updates_latest_url = await redis_client.execute('GET', updates_latest_url_key)
            if updates_latest_url is not None:
                url = updates_latest_url
                break

            await sleep(logger, 1)

    return url.decode('utf-8')


async def sleep(logger, interval):
    with logged(logger, 'Sleeping for %s seconds', [interval]):
        await asyncio.sleep(interval)


async def ingest_feed_page(logger, metrics, session, ingest_type, feed_lock, feed, es_endpoint,
                           index_names, href):
    with \
            logged(logger, 'Polling/pushing page', []), \
            metric_timer(metrics['ingest_page_duration_seconds'],
                         [feed.unique_id, ingest_type, 'total']):

        with \
                logged(logger, 'Polling page (%s)', [href]), \
                metric_timer(metrics['ingest_page_duration_seconds'],
                             [feed.unique_id, ingest_type, 'pull']):
            # Lock so there is only 1 request per feed at any given time
            async with feed_lock:
                feed_contents = await get_feed_contents(session, href, feed.auth_headers(href))

        with logged(logger, 'Parsing JSON', []):
            feed_parsed = json.loads(feed_contents)

        with logged(logger, 'Converting to bulk Elasticsearch items', []):
            es_bulk_items = feed.convert_to_bulk_es(feed_parsed, index_names)

        with \
                metric_timer(metrics['ingest_page_duration_seconds'],
                             [feed.unique_id, ingest_type, 'push']), \
                metric_counter(metrics['ingest_activities_nonunique_total'],
                               [feed.unique_id], len(es_bulk_items)):
            await es_bulk(logger, session, es_endpoint, es_bulk_items)

        return feed.next_href(feed_parsed)


async def get_feed_contents(session, href, headers):
    async with session.get(href, headers=headers) as result:
        if result.status != 200:
            raise Exception(await result.text())

        return await result.read()


def parse_feed_config(feed_config):
    by_feed_type = {
        'activity_stream': ActivityStreamFeed,
        'zendesk': ZendeskFeed,
    }
    return by_feed_type[feed_config['TYPE']].parse_config(feed_config)


async def create_metrics_application(parent_logger, metrics, metrics_registry, redis_client,
                                     raven_client, session, feed_endpoints, es_endpoint):
    logger = get_child_logger(parent_logger, 'metrics')

    async def poll_metrics():
        with logged(logger, 'Polling', []):
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
                    searchable, nonsearchable = await es_feed_activities_total(
                        logger, session, es_endpoint, feed_id)
                    metrics['elasticsearch_feed_activities_total'].labels(
                        feed_id, 'searchable').set(searchable)
                    metrics['elasticsearch_feed_activities_total'].labels(
                        feed_id, 'nonsearchable').set(nonsearchable)
                except ESMetricsUnavailable:
                    pass

        await save_metrics_to_redis(logger, generate_latest(metrics_registry))
        await sleep(logger, METRICS_INTERVAL)

    async def save_metrics_to_redis(logger, metrics):
        with logged(logger, 'Saving to Redis', []):
            await redis_client.set('metrics', metrics)

    asyncio.get_event_loop().create_task(
        async_repeat_until_cancelled(logger, raven_client, METRICS_INTERVAL, poll_metrics)
    )


async def set_metric_if_can(metric, labels, get_value_coroutine):
    try:
        metric.labels(*labels).set(await get_value_coroutine)
    except ESMetricsUnavailable:
        pass


if __name__ == '__main__':
    main(run_outgoing_application)
