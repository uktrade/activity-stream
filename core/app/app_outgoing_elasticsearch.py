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
    es_request,
    es_request_non_200_exception,
)
from .metrics import (
    metric_timer,
)

DELETE_TIMEOUTS = [1, 2, 4, 8, 16, 32] + [64] * 120


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
        f'{ALIAS_ACTIVITIES_SCHEMAS}__'
        f'feed_id_{feed_unique_id}__'
        f'date_{today}__'
        f'timestamp_{now}__'
        f'batch_id_{unique}__',
        ''
        f'{ALIAS_OBJECTS}__'
        f'feed_id_{feed_unique_id}__'
        f'date_{today}__'
        f'timestamp_{now}__'
        f'batch_id_{unique}__',
        f'{ALIAS_OBJECTS_SCHEMAS}__'
        f'feed_id_{feed_unique_id}__'
        f'date_{today}__'
        f'timestamp_{now}__'
        f'batch_id_{unique}__'
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
        if index_name.startswith(f'{ALIAS_ACTIVITIES_SCHEMAS}__')
    ], [
        index_name for index_name in index_names
        if index_name.startswith(f'{ALIAS_OBJECTS}__')
    ], [
        index_name for index_name in index_names
        if index_name.startswith(f'{ALIAS_OBJECTS_SCHEMAS}__')
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
            path=f'/_aliases',
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


def to_schemas(objects):
    def _maybe_collapse_list(list_instance):
        # Without this, we end up with lots of "almost" identical schemas. This does lose
        # information, but hopefully still makes the schema helpful.
        return \
            'list-of-strings' if all((item == 'string' for item in list_instance)) else \
            'list-of-dicts' if all((isinstance(item, dict) for item in list_instance)) else \
            list_instance

    def _to_schema(obj):
        return \
            {
                key: _to_schema(value)
                for key, value in obj.items() if value is not None
            } if isinstance(obj, dict) else \
            _maybe_collapse_list([
                _to_schema(value)
                for value in obj
                if value if not None
            ]) if isinstance(obj, list) else \
            'string' if isinstance(obj, str) else \
            'number' if isinstance(obj, (int, float)) else \
            'unknown'

    def _to_schema_id_and_json(obj):
        schema_json = json_dumps(_to_schema(obj))
        return (
            'schema:{:02x}'.format(hash(schema_json)),
            schema_json,
        )

    return (_to_schema_id_and_json(obj) for obj in objects)


async def add_remove_aliases_atomically(context,
                                        activity_index_name, activity_objects_index_name,
                                        object_index_name, object_schemas_index_name,
                                        feed_unique_id):
    with logged(context.logger.debug, context.logger.warning,
                'Atomically flipping {ALIAS_ACTIVITIES} alias to (%s)',
                [feed_unique_id]):
        activities_remove_pattern = f'{ALIAS_ACTIVITIES}__feed_id_{feed_unique_id}__*'
        activities_schemas_remove_pattern = \
            f'{ALIAS_ACTIVITIES_SCHEMAS}__feed_id_{feed_unique_id}__*'
        objects_remove_pattern = f'{ALIAS_OBJECTS}__feed_id_{feed_unique_id}__*'
        objects_schemas_remove_pattern = f'{ALIAS_OBJECTS_SCHEMAS}__feed_id_{feed_unique_id}__*'
        actions = json_dumps({
            'actions': [
                {'remove': {'index': activities_remove_pattern,
                            'alias': ALIAS_ACTIVITIES}},
                {'remove': {'index': activities_schemas_remove_pattern,
                            'alias': ALIAS_ACTIVITIES_SCHEMAS}},
                {'remove': {'index': objects_remove_pattern,
                            'alias': ALIAS_OBJECTS}},
                {'remove': {'index': objects_schemas_remove_pattern,
                            'alias': ALIAS_OBJECTS_SCHEMAS}},
                {'add': {'index': activity_index_name, 'alias': ALIAS_ACTIVITIES}},
                {'add': {'index': activity_objects_index_name, 'alias': ALIAS_ACTIVITIES_SCHEMAS}},
                {'add': {'index': object_index_name, 'alias': ALIAS_OBJECTS}},
                {'add': {'index': object_schemas_index_name, 'alias': ALIAS_OBJECTS_SCHEMAS}},
            ]
        })

        await es_request_non_200_exception(
            context=context,
            method='POST',
            path=f'/_aliases',
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
            for i, timeout in enumerate(DELETE_TIMEOUTS):
                try:
                    await es_request_non_200_exception(
                        context=context,
                        method='DELETE',
                        path=f'/{index_name}',
                        query={},
                        headers={'Content-Type': 'application/json'},
                        payload=b'',
                    )
                except ESNon200Exception as exception:
                    status = exception.args[1]
                    context.logger.exception('Failed index DELETE of (%s)', [index_name])
                    if 400 <= status < 500:
                        failed_index_names.append(index_name)
                        break
                    await sleep(context, timeout)
                except Exception:
                    context.logger.exception('Failed index DELETE of (%s)', [index_name])
                    if i == len(DELETE_TIMEOUTS):
                        failed_index_names.append(index_name)
                        continue
                    await sleep(context, timeout)
                else:
                    break

        if failed_index_names:
            raise Exception('Failed DELETE of indexes ({})'.format(failed_index_names))


async def create_activities_index(context, index_name):
    with logged(context.logger.debug, context.logger.warning, 'Creating index (%s)', [index_name]):
        index_definition = json_dumps({
            'settings': {
                'index': {
                    'number_of_shards': 3,
                    'number_of_replicas': 1,
                    'refresh_interval': '-1',
                }
            },
            'mappings': {
                '_doc': {
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
                        'object.attributedTo.id': {
                            'type': 'keyword',
                        },
                        'object.attributedTo.type': {
                            'type': 'keyword',
                        },
                        'actor.dit:companiesHouseNumber': {
                            'type': 'keyword',
                        },
                    },
                },
            },
        })
        await es_request_non_200_exception(
            context=context,
            method='PUT',
            path=f'/{index_name}',
            query={},
            headers={'Content-Type': 'application/json'},
            payload=index_definition,
        )


async def create_objects_index(context, index_name):
    with logged(context.logger.debug, context.logger.warning, 'Creating index (%s)', [index_name]):
        index_definition = json_dumps({
            'settings': {
                'index': {
                    'number_of_shards': 3,
                    'number_of_replicas': 1,
                    'refresh_interval': '-1',
                }
            },
            'mappings': {
                '_doc': {
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
                        'attributedTo.id': {
                            'type': 'keyword',
                        },
                        'attributedTo.type': {
                            'type': 'keyword',
                        },
                    },
                },
            },
        })
        await es_request_non_200_exception(
            context=context,
            method='PUT',
            path=f'/{index_name}',
            query={},
            headers={'Content-Type': 'application/json'},
            payload=index_definition,
        )


async def create_schemas_index(context, index_name):
    with logged(context.logger.debug, context.logger.warning, 'Creating index (%s)', [index_name]):
        index_definition = json_dumps({
            'settings': {
                'index': {
                    'number_of_shards': 1,
                    'number_of_replicas': 0,
                    'refresh_interval': '-1',
                }
            },
            'mappings': {
                '_doc': {
                    'dynamic': False,
                },
            }
        })
        await es_request_non_200_exception(
            context=context,
            method='PUT',
            path=f'/{index_name}',
            query={},
            headers={'Content-Type': 'application/json'},
            payload=index_definition,
        )


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


async def es_ingest_schemas(context, schemas, schema_index_names):
    with logged(context.logger.debug, context.logger.warning,
                'Pushing (%s) schemas into Elasticsearch', [len(schemas)]):
        if not schemas:
            return

        with logged(context.logger.debug, context.logger.warning,
                    'Converting to Elasticsearch bulk ingest commands', []):
            es_bulk_contents = b''.join(
                flatten_generator(
                    [
                        json_dumps({
                            'index': {
                                '_id': schema_id,
                                '_index': schema_index_name,
                                '_type': '_doc',
                            }
                        }),
                        b'\n',
                        schema_json,
                        b'\n',
                    ]
                    for schema_id, schema_json in schemas
                    for schema_index_name in schema_index_names
                ),
            )

        await _es_bulk_post(context, es_bulk_contents)


async def _es_bulk_post(context, es_bulk_contents):
    with logged(context.logger.debug, context.logger.warning,
                'POSTing bulk ingest to Elasticsearch', []):
        await es_request_non_200_exception(
            context=context, method='POST', path='/_bulk', query={},
            headers={'Content-Type': 'application/x-ndjson'}, payload=es_bulk_contents,
        )


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
