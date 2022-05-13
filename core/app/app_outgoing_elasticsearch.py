import asyncio
import datetime
import itertools

from .logger import (
    logged,
)
from .app_outgoing_utils import (
    flatten,
    flatten_generator,
)
from .utils import (
    json_dumps,
    json_loads,
    random_url_safe,
    sleep,
)
from .elasticsearch import (
    ALIAS_ACTIVITIES,
    ALIAS_ACTIVITIES_SCHEMAS,
    ALIAS_OBJECTS,
    ALIAS_OBJECTS_SCHEMAS,
    ESNon200Exception,
    ESMetricsUnavailable,
    es_mappings,
    es_request,
    es_request_non_200_exception,
)
from .metrics import (
    metric_timer,
)

RETRY_TIMEOUTS = [1, 2, 4, 8, 16, 32] + [64] * 20


def get_new_index_names(feed_unique_id):
    today = datetime.date.today().isoformat()
    now = str(datetime.datetime.now().timestamp()).split('.')[0]
    unique = random_url_safe(8)

    # Storing metadata in index name allows operations to match on
    # them, both by elasticsearch itself, and by regex in Python
    return (
        ''
        f'{ALIAS_ACTIVITIES}__'
        f'feed_id_{feed_unique_id}__'
        f'date_{today}__'
        f'timestamp_{now}__'
        f'batch_id_{unique}__',
        f'{ALIAS_OBJECTS}__'
        f'feed_id_{feed_unique_id}__'
        f'date_{today}__'
        f'timestamp_{now}__'
        f'batch_id_{unique}__',
    )


def indexes_matching_feeds(index_names, feed_unique_ids):
    return flatten([
        indexes_matching_feed(index_names, feed_unique_id)
        for feed_unique_id in feed_unique_ids
    ])


def indexes_matching_feed(index_names, feed_unique_id):
    return [
        index_name
        for index_name in index_names
        if (
            index_name.startswith(f'{ALIAS_ACTIVITIES}__feed_id_{feed_unique_id}__') or
            index_name.startswith(f'{ALIAS_ACTIVITIES_SCHEMAS}__feed_id_{feed_unique_id}__') or
            index_name.startswith(f'{ALIAS_OBJECTS}__feed_id_{feed_unique_id}__') or
            index_name.startswith(f'{ALIAS_OBJECTS_SCHEMAS}__feed_id_{feed_unique_id}__')
        )
    ]


def indexes_matching_no_feeds(index_names, feed_unique_ids):
    indexes_matching = indexes_matching_feeds(index_names, feed_unique_ids)
    return [
        index_name
        for index_name in index_names
        if index_name not in indexes_matching
    ]


def split_index_names(index_names):
    return [
        index_name for index_name in index_names
        if index_name.startswith(f'{ALIAS_ACTIVITIES}__')
    ], [
        index_name for index_name in index_names
        if index_name.startswith(f'{ALIAS_OBJECTS}__')
    ]


async def get_old_index_names(context):
    def is_activity_stream_index(index_name):
        return (
            index_name.startswith(f'{ALIAS_ACTIVITIES}__') or
            index_name.startswith(f'{ALIAS_ACTIVITIES_SCHEMAS}__') or
            index_name.startswith(f'{ALIAS_OBJECTS}__') or
            index_name.startswith(f'{ALIAS_OBJECTS_SCHEMAS}__')
        )

    with logged(context.logger.debug, context.logger.warning, 'Finding existing index names', []):
        results = await es_request_non_200_exception(
            context=context,
            method='GET',
            path='/_aliases',
            query={},
            headers={'Content-Type': 'application/json'},
            payload=b'',
        )
        indexes = json_loads(results._body)

        without_alias = [
            index_name
            for index_name, index_details in indexes.items()
            if is_activity_stream_index(index_name) and not index_details['aliases']
        ]
        with_alias = [
            index_name
            for index_name, index_details in indexes.items()
            if is_activity_stream_index(index_name) and index_details['aliases']
        ]
        names = without_alias, with_alias
        context.logger.debug('Finding existing index names... (%s)', names)
        return names


async def add_remove_aliases_atomically(context,
                                        activity_index_name,
                                        object_index_name,
                                        feed_unique_id):
    with logged(context.logger.debug, context.logger.warning,
                'Atomically flipping {ALIAS_ACTIVITIES} alias to (%s)',
                [feed_unique_id]):
        activities_remove_pattern = f'{ALIAS_ACTIVITIES}__feed_id_{feed_unique_id}__*'
        objects_remove_pattern = f'{ALIAS_OBJECTS}__feed_id_{feed_unique_id}__*'
        actions = json_dumps({
            'actions': [
                {'remove': {'index': activities_remove_pattern,
                            'alias': ALIAS_ACTIVITIES}},
                {'remove': {'index': objects_remove_pattern,
                            'alias': ALIAS_OBJECTS}},
                {'add': {'index': activity_index_name, 'alias': ALIAS_ACTIVITIES}},
                {'add': {'index': object_index_name, 'alias': ALIAS_OBJECTS}},
            ]
        })

        await es_request_non_200_exception(
            context=context,
            method='POST',
            path='/_aliases',
            query={},
            headers={'Content-Type': 'application/json'},
            payload=actions,
        )


async def delete_indexes(context, index_names):
    # If the snapshot is taking a long time, it's likely large, and so
    # its _more_ important to wait until its deleted before continuing with
    # ingest. If we still can't delete, we abort and raise
    #
    # We might have a lot of indexes to delete, and we make as much effort
    # as possible to delete as many of them as possible before raising

    with logged(context.logger.debug, context.logger.warning, 'Deleting indexes (%s)',
                [index_names]):
        if not index_names:
            return
        failed_index_names = []
        for index_name in index_names:
            for i, timeout in enumerate(RETRY_TIMEOUTS):
                try:
                    await es_request_non_200_exception(
                        context=context,
                        method='DELETE',
                        path=f'/{index_name}',
                        query={},
                        headers={'Content-Type': 'application/json'},
                        payload=b'',
                    )
                except Exception:
                    context.logger.exception('Failed index DELETE of (%s)', [index_name])
                    if i == len(RETRY_TIMEOUTS) - 1:
                        failed_index_names.append(index_name)
                        break
                    await sleep(context, timeout)
                else:
                    break

        if failed_index_names:
            raise Exception('Failed DELETE of indexes ({})'.format(failed_index_names))

    await wait_for_indexes_to_delete(context, index_names)


async def wait_for_indexes_to_delete(context, index_names):
    max_attempts = 60
    with logged(context.logger.debug, context.logger.warning, 'Waiting for indexes to delete (%s)',
                [index_names]):
        for index_name in index_names:
            for i in range(0, max_attempts):
                response = await es_request(
                    context=context,
                    method='HEAD',
                    path=f'/{index_name}',
                    query={},
                    headers={'Content-Type': 'application/json'},
                    payload=b'',
                )
                if response.status == 404:
                    continue
                if i == max_attempts - 1:
                    raise Exception(f'Failed waiting for deletion of index ({index_name})')
                await asyncio.sleep(2)


async def create_activities_index(context, index_name):
    num_primary_shards = 3
    num_replicas_per_shard = 1

    with logged(context.logger.debug, context.logger.warning, 'Creating index (%s)', [index_name]):
        index_definition = json_dumps({
            'settings': {
                'analysis': {
                    'normalizer': {
                        'my_normalizer': {
                            'type': 'custom',
                            'char_filter': [],
                            'filter': ['lowercase']
                        }
                    },
                },
                'index': {
                    'number_of_shards': num_primary_shards,
                    'number_of_replicas': num_replicas_per_shard,
                    'refresh_interval': '-1',
                },
            },
            'mappings': es_mappings({
                'dynamic': False,
                'properties': {
                    'id': {
                        'type': 'keyword',
                    },
                    'published': {
                        'type': 'date',
                    },
                    'type': {
                        'type': 'keyword',
                    },
                    'startTime': {
                        'type': 'date',
                    },
                    'endTime': {
                        'type': 'date',
                    },
                    'generator.name': {
                        'type': 'keyword',
                    },
                    'object.type': {
                        'type': 'keyword',
                    },
                    'object.published': {
                        'type': 'date',
                    },
                    'object.content': {
                        'type': 'text',
                    },
                    'object.name': {
                        'type': 'text',
                    },
                    'object.startTime': {
                        'type': 'date',
                    },
                    'object.endTime': {
                        'type': 'date',
                    },
                    # Not AS 2.0, but is used, and is a space-separated
                    # list of keywords
                    'object.keywords': {
                        'type': 'text',
                    },
                    'object.dit:emailAddress': {
                        'type': 'keyword',
                        'normalizer': 'my_normalizer',
                    },
                    'object.url': {
                        'type': 'keyword',
                    },
                    'object.attributedTo.id': {
                        'type': 'keyword',
                    },
                    'object.attributedTo.type': {
                        'type': 'keyword',
                    },
                    'actor.dit:companiesHouseNumber': {
                        'type': 'keyword',
                    },
                    'actor.dit:emailAddress': {
                        'type': 'keyword',
                    },
                    'object.geocoordinates': {
                        'type': 'geo_point'
                    },
                    'object.dit:status': {
                        'type': 'keyword'
                    },
                    'object.dit:public': {
                        'type': 'boolean'
                    }
                },
            }),
        })
        await es_request_non_200_exception(
            context=context,
            method='PUT',
            path=f'/{index_name}',
            query={},
            headers={'Content-Type': 'application/json'},
            payload=index_definition,
        )
        await wait_until_num_shards(context, index_name, num_primary_shards,
                                    num_replicas_per_shard)


async def create_objects_index(context, index_name):
    num_primary_shards = 3
    num_replicas_per_shard = 1

    with logged(context.logger.debug, context.logger.warning, 'Creating index (%s)', [index_name]):
        index_definition = json_dumps({
            'settings': {
                'index': {
                    'number_of_shards': num_primary_shards,
                    'number_of_replicas': num_replicas_per_shard,
                    'refresh_interval': '-1',
                }
            },
            'mappings': es_mappings({
                'dynamic': False,
                'properties': {
                    'id': {
                        'type': 'keyword',
                    },
                    'published': {
                        'type': 'date',
                    },
                    'type': {
                        'type': 'keyword',
                    },
                    'content': {
                        'type': 'text',
                    },
                    'name': {
                        'type': 'text',
                    },
                    'startTime': {
                        'type': 'date',
                    },
                    'endTime': {
                        'type': 'date',
                    },
                    # Not AS 2.0, but is used, and is a space-separated
                    # list of keywords
                    'keywords': {
                        'type': 'text',
                    },
                    'dit:emailAddress': {
                        'type': 'keyword',
                    },
                    'url': {
                        'type': 'keyword',
                    },
                    'attributedTo.id': {
                        'type': 'keyword',
                    },
                    'attributedTo.type': {
                        'type': 'keyword',
                    },
                    'geocoordinates': {
                        'type': 'geo_point'
                    },
                    'dit:status': {
                        'type': 'keyword'
                    },
                    'dit:public': {
                        'type': 'boolean'
                    }
                },
            }),
        })
        await es_request_non_200_exception(
            context=context,
            method='PUT',
            path=f'/{index_name}',
            query={},
            headers={'Content-Type': 'application/json'},
            payload=index_definition,
        )
        await wait_until_num_shards(context, index_name, num_primary_shards,
                                    num_replicas_per_shard)


async def wait_until_num_shards(context, index_name, num_primary_shards, num_replicas_per_shard):
    # Have witnessed the default number of shards of 5 created even if requested less, and have
    # witnessed replicas created even if requested 0 which then get filled with documents.

    # Check how many shards we actually can start based on the number of nodes. This is mostly
    # for a test environment when we can't start enough replicas
    response = await es_request_non_200_exception(
        context=context,
        method='GET',
        path='/_cat/nodes',
        query={'format': 'json'},
        headers={'Content-Type': 'application/json'},
        payload=b'',
    )
    num_nodes = len(json_loads(response._body))
    num_desired_shards = num_primary_shards + num_primary_shards * num_replicas_per_shard

    max_allowed_replicas_per_shard = min(num_nodes - 1, num_replicas_per_shard)
    num_expected_started_shards = \
        num_primary_shards + num_primary_shards * max_allowed_replicas_per_shard
    num_expected_non_started_shards = num_desired_shards - num_expected_started_shards

    with logged(context.logger.debug, context.logger.warning,
                'Waiting until correct number of shards (%s)', [index_name]):
        for _ in range(0, 60):
            response = await es_request_non_200_exception(
                context=context,
                method='GET',
                path=f'/_cat/shards/{index_name}',
                query={'format': 'json'},
                headers={'Content-Type': 'application/json'},
                payload=b'',
            )
            shard_dicts = json_loads(response._body)
            num_shards_started = len([
                shard_dict for shard_dict in shard_dicts
                if shard_dict['state'] == 'STARTED'
            ])
            num_non_started_shards = len([
                shard_dict for shard_dict in shard_dicts
                if shard_dict['state'] != 'STARTED'
            ])
            if num_shards_started == num_expected_started_shards \
                    and num_non_started_shards == num_expected_non_started_shards:
                return
            await asyncio.sleep(2)

        raise Exception(f'Unable to create index with correct number of shards {index_name}')


async def refresh_index(context, index_name, *metric_labels):
    metric = context.metrics['elasticsearch_refresh_duration_seconds']
    with \
            logged(context.logger.debug, context.logger.warning, 'Refreshing index (%s)',
                   [index_name]), \
            metric_timer(metric, [metric_labels]):

        await es_request_non_200_exception(
            context=context,
            method='POST',
            path=f'/{index_name}/_refresh',
            query={'ignore_unavailable': 'true'},
            headers={'Content-Type': 'application/json'},
            payload=b'',
        )


async def es_bulk_ingest(context, activities, activity_index_names, object_index_names):
    with logged(context.logger.debug, context.logger.warning,
                'Pushing (%s) activities into Elasticsearch', [len(activities)]):
        if not activities:
            return

        with logged(context.logger.debug, context.logger.warning,
                    'Converting to Elasticsearch bulk ingest commands', []):
            es_bulk_contents = b''.join(itertools.chain(
                flatten_generator(
                    [
                        json_dumps({
                            'index': {
                                '_id': activity['id'],
                                '_index': activity_index_name,
                                '_type': '_doc',
                            }
                        }),
                        b'\n',
                        activity_json,
                        b'\n',
                    ]
                    for activity in activities
                    for activity_json in [json_dumps(activity)]
                    for activity_index_name in activity_index_names
                ),
                flatten_generator(
                    [
                        json_dumps({
                            'index': {
                                '_id': activity['object']['id'],
                                '_index': object_index_name,
                                '_type': '_doc',
                            }
                        }),
                        b'\n',
                        object_json,
                        b'\n',
                    ]
                    for activity in activities
                    for object_json in [json_dumps(activity['object'])]
                    for object_index_name in object_index_names
                ),
            ))

        await _es_bulk_post(context, es_bulk_contents)


async def _es_bulk_post(context, es_bulk_contents):
    for i, timeout in enumerate(RETRY_TIMEOUTS):
        try:
            with logged(context.logger.debug, context.logger.warning,
                        'POSTing bulk ingest to Elasticsearch', []):
                await es_request_non_200_exception(
                    context=context, method='POST', path='/_bulk', query={},
                    headers={'Content-Type': 'application/x-ndjson'}, payload=es_bulk_contents,
                )
        except (asyncio.TimeoutError, ESNon200Exception):
            if i == len(RETRY_TIMEOUTS) - 1:
                raise
            await sleep(context, timeout)
        else:
            return


async def es_searchable_total(context):
    # This metric is expected to be available
    searchable_result = await es_request_non_200_exception(
        context=context,
        method='GET',
        path=f'/{ALIAS_ACTIVITIES}/_count',
        query={'ignore_unavailable': 'true'},
        headers={'Content-Type': 'application/json'},
        payload=b'',
    )
    return json_loads(searchable_result._body)['count']


async def es_nonsearchable_total(context):
    nonsearchable_result = await es_maybe_unvailable_metrics(
        context=context,
        method='GET',
        path=f'/{ALIAS_ACTIVITIES}_*,-*{ALIAS_ACTIVITIES}/_count',
        query={'ignore_unavailable': 'true'},
        headers={'Content-Type': 'application/json'},
        payload=b'',
    )
    return json_loads(nonsearchable_result._body)['count']


async def es_feed_activities_total(context, feed_id):
    nonsearchable_result = await es_maybe_unvailable_metrics(
        context=context,
        method='GET',
        path=f'/{ALIAS_ACTIVITIES}__feed_id_{feed_id}__*,-*{ALIAS_ACTIVITIES}/_count',
        query={'ignore_unavailable': 'true'},
        headers={'Content-Type': 'application/json'},
        payload=b'',
    )
    nonsearchable = json_loads(nonsearchable_result._body)['count']

    total_results = await es_maybe_unvailable_metrics(
        context=context,
        method='GET',
        path=f'/{ALIAS_ACTIVITIES}__feed_id_{feed_id}__*/_count',
        query={'ignore_unavailable': 'true'},
        headers={'Content-Type': 'application/json'},
        payload=b'',
    )
    searchable = max(json_loads(total_results._body)['count'] - nonsearchable, 0)

    return searchable, nonsearchable


async def es_maybe_unvailable_metrics(context, method, path, query, headers, payload):
    ''' Some metrics may be unavailable since they query newly created indexes '''
    results = await es_request(context, method, path, query, headers, payload)
    if results.status == 503:
        raise ESMetricsUnavailable()
    if results.status != 200:
        raise Exception(results._body.decode('utf-8'))
    return results
