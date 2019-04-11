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
    es_bulk_ingest,
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
from .dns import (
    AioHttpDnsResolver,
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
    """Indefinitely poll paginated feeds and ingest them into Elasticsearch

    As part of startup:

    - Read environment variables that specifify what feeds to poll, with any
      authentication credentials
    - Create HTTPS and Redis connection pools
    - Create a raven client for reporting errors to Sentry
    - Create a logging "context" that allows child contexts to be created
      from, which allow the same function to log output slightly differently
      when run from different tasks
    - Create the metrics registry in which functions throught the application
      store metrics. This registry which is then periodically exported from by
      the metrics application, also started here, into Redis.

    Exceptions are raised if any of the above fails in order to fail
    blue/green deployments. Other error cases are swallowed and retried after
    intervals in EXCEPTION_INTERVALS.

    A lock is acquired before any connections to feeds or Elasticsearch to
    prevent conflicts with the existing version of the outgoing application
    during blue/green deployment.

    Once the above is done, a task that performs the polling and ingest is
    created.

    A cleanup function is returned that is expected to be called just before
    the application is shut down to given every chance for operation to
    shutdown cleanly.
    """
    logger = get_root_logger('outgoing')

    with logged(logger, 'Examining environment', []):
        env = normalise_environment(os.environ)
        es_uri, redis_uri, sentry = get_common_config(env)
        feeds = [parse_feed_config(feed) for feed in env['FEEDS']]

    settings.ES_URI = es_uri
    conn = aiohttp.TCPConnector(use_dns_cache=False, resolver=AioHttpDnsResolver())
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
    await create_outgoing_application(context, feeds)
    await create_metrics_application(
        context, metrics_registry, feeds,
    )

    async def cleanup():
        await cancel_non_current_tasks()

        redis_client.close()
        await redis_client.wait_closed()

        await session.close()
        # https://github.com/aio-libs/aiohttp/issues/1925
        await asyncio.sleep(0.250)

    return cleanup


async def create_outgoing_application(context, feeds):
    """Create a task that polls feeds and ingests them into Elasticsearch

    This task is repeated in case there is some issue at the beginning of
    `ingest_feeds` that causes an exception to be raised. There is an argument
    that it is better to bubble such exceptions in order to fail deployments,
    but this is not yet decided.
    """
    asyncio.get_event_loop().create_task(
        repeat_until_cancelled(
            context, EXCEPTION_INTERVALS,
            to_repeat=ingest_feeds, to_repeat_args=(context, feeds),
        )
    )


async def ingest_feeds(context, feeds):
    """Create tasks that poll feeds and ingest them into Elasticsearch

    This deletes any unused indexes, for example for feeds that used to be
    configured. It then repeats the "full" and "updates" ingest cycles for
    all feeds until cancellation, which is expected to only be just before the
    application closes down.

    Two tasks are created for each feed, a "full" task for the full ingest
    and an "updates" task for the updates ingest.
    """
    all_feed_ids = [feed.unique_id for feed in feeds]
    indexes_without_alias, indexes_with_alias = await get_old_index_names(context)

    indexes_to_delete = indexes_matching_no_feeds(
        indexes_without_alias + indexes_with_alias, all_feed_ids)
    await delete_indexes(
        get_child_context(context, 'initial-delete'), indexes_to_delete,
    )

    await asyncio.gather(*[
        repeat_until_cancelled(
            context, feed.exception_intervals,
            to_repeat=ingest_func, to_repeat_args=(context, feed),
        )
        for feed in feeds
        for ingest_func in (ingest_full, ingest_updates)
    ])


async def ingest_full(parent_context, feed):
    """Perform a single "full" ingest cycle of a paginated source feed

    Starting at feed.seed, iteratively request all pages from the feed, and
    ingest each into Elasticsearch. Indexes are created specifically for this
    ingest cycle, with unused indexes deleted.

    At the end of the cycle the `activities` and `objects` index aliases
    are flipped so that they now alias the indexes created and ingested into
    in this cycle, i.e. made visible to clients of the incoming app that only
    query the `activities` and `objects` aliases.

    This is a "partial" flip of the aliases, since they continue to also alias
    indexes from _other_ feeds. This design allows the presentation of single
    `activities` and `objects` indexes to clients, but also allows the ingest
    cycle of feeds to fail without affecting the ingest of other feeds.

    Unused indexes are deleted at the beginning of a cycle rather than the
    end, to always clean up after any unexpected shutdown _before_ ingesting
    more data.

    The primary purpose of always ingesting into new indexes and then flipping
    aliases is to allow for hard-deletion without any explicit "deletion" code
    path. It also allows for data format changes/corrections.
    """
    context = get_child_context(parent_context, f'{feed.unique_id},full')
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
            href = await fetch_and_ingest_page(
                context, 'full', feed, [activities_index_name], [objects_index_name], href,
            )
            await sleep(context, feed.full_ingest_page_interval)

        await refresh_index(context, activities_index_name)
        await refresh_index(context, objects_index_name)
        await add_remove_aliases_atomically(
            context, activities_index_name, objects_index_name, feed.unique_id)
        await set_feed_updates_seed_url(context, feed.unique_id, updates_href)


async def ingest_updates(parent_context, feed):
    """Perform a single "updates" ingest cycle of a paginated source feed

    Poll the last page from the last completed "full" ingest and ingest it
    into Elasticsearch.

    This past page is paginated: if it has a next page, it too is fetched and
    ingested from. This repeated until a page _without_ a next page, which is
    then made the target of the polling.

    Data is ingested into two sets of indexes. 1) The indexes that are aliased
    to `activities` and `objects`, so they are immediately visible to clients
    of the incoming app. 2) The target of the current "full" ingest. This is
    to avoid the race condition:

    - An activity has been ingested and visible in the incoming app

    - The last page of data has been fetched during the "full" ingest, but
      the alias flip has not yet occurred.

    - A change is made to the activity, and the "updates" ingests it, and so
      is visible in the incoming app

    - The alias flip from the full ingest is performed, and the pre-change
      version of the activity is visible in the incoming app.

    This would "correct" on the next full ingest, but it has been deemed
    strange and unexpected enough to ensure it doesn't happen.
    """
    context = get_child_context(parent_context, f'{feed.unique_id},updates')
    metrics = context.metrics
    with \
            logged(context.logger, 'Updates ingest', []), \
            metric_timer(metrics['ingest_feed_duration_seconds'], [feed.unique_id, 'updates']):

        href = await get_feed_updates_url(context, feed.unique_id)
        indexes_without_alias, indexes_with_alias = await get_old_index_names(context)

        # We deliberately ingest into both the live and ingesting indexes
        indexes_to_ingest_into = indexes_matching_feeds(
            indexes_without_alias + indexes_with_alias, [feed.unique_id])

        activities_index_names, objects_index_names = split_index_names(indexes_to_ingest_into)

        while href:
            updates_href = href
            href = await fetch_and_ingest_page(
                context, 'updates', feed, activities_index_names, objects_index_names, href,
            )

        for index_name in indexes_matching_feeds(indexes_with_alias, [feed.unique_id]):
            await refresh_index(context, index_name)
        await set_feed_updates_url(context, feed.unique_id, updates_href)

    await sleep(context, feed.updates_page_interval)


async def fetch_and_ingest_page(context, ingest_type, feed, activity_index_names,
                                objects_index_names, href):
    """Ingest a page into Elasticsearch by calling fetch_page + es_bulk_ingest

    The url of the next page is returned or `None` if there is no next page
    """
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
                feed_contents = await fetch_page(
                    context, href, await feed.auth_headers(context, href),
                )

        with logged(context.logger, 'Parsing JSON', []):
            feed_parsed = ujson.loads(feed_contents)

        with logged(context.logger, 'Converting to activities', []):
            activities = await feed.get_activities(context, feed_parsed)

        num_es_documents = len(activities) * (len(activity_index_names) + len(objects_index_names))
        with \
                metric_timer(context.metrics['ingest_page_duration_seconds'],
                             [feed.unique_id, ingest_type, 'push']), \
                metric_counter(context.metrics['ingest_activities_nonunique_total'],
                               [feed.unique_id, ingest_type], num_es_documents):
            await es_bulk_ingest(context, activities, activity_index_names, objects_index_names)

        asyncio.ensure_future(set_feed_status(
            context, feed.unique_id, feed.down_grace_period, b'GREEN'))

        return feed.next_href(feed_parsed)


async def fetch_page(context, href, headers):
    """Fetch a single page of data from a feed, returning it as bytes

    If a non-200 response is returned, an exception is raised. However, if a
    429 is returned with a Retry-After header, the fetch is retried after this
    time, up to 10 attempts. After 10 failed attempts, an exception is
    raised.

    Raised exceptions are expected to cause the current ingest cycle to fail,
    but be re-attempted some time later.
    """
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


async def create_metrics_application(parent_context, metrics_registry, feeds):
    """Creates a task that supplies metrics to the incoming application

    Every METRICS_INTERVAL seconds the metrics are exported to Redis, so that
    they are available to the incoming app, at an endpoint which is queried
    by Prometheus and then used in Grafana. This is slightly awkward, but no
    better way has been thought of.
    """
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

            feed_ids = [feed.unique_id for feed in feeds]
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
