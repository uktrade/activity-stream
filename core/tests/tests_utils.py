import asyncio
import json
import os

import aiohttp
from aiohttp import web
import aioredis
import mohawk

from ..app.app_incoming import run_incoming_application
from ..app.app_outgoing import run_outgoing_application


ORIGINAL_SLEEP = asyncio.sleep


async def fast_sleep(_):
    await ORIGINAL_SLEEP(0.5)


def async_test(func):
    def wrapper(*args, **kwargs):
        future = func(*args, **kwargs)
        loop = asyncio.get_event_loop()
        loop.run_until_complete(future)
    return wrapper


async def run_app_until_accepts_http():
    cleanup_inc = await run_incoming_application()
    cleanup_out = await run_outgoing_application()
    await is_http_accepted_eventually()

    async def cleanup():
        await cleanup_inc()
        await cleanup_out()
    return cleanup


async def is_http_accepted_eventually():
    attempts = 0
    while attempts < 20:
        try:
            async with aiohttp.ClientSession() as session:
                url = 'http://127.0.0.1:8080/'
                await session.get(url, data='{}', timeout=1)
            return True
        except aiohttp.client_exceptions.ClientConnectorError:
            attempts += 1
            await ORIGINAL_SLEEP(1)


async def wait_until_get_working():
    # Assume can already connect on HTTP
    attempts = 0
    while attempts < 50:
        async with aiohttp.ClientSession() as session:
            url = 'http://127.0.0.1:8080/v2/activities'
            auth = hawk_auth_header(
                'incoming-some-id-3', 'incoming-some-secret-3', url,
                'GET', '{}', 'application/json',
            )
            result = await session.get(url, headers={
                'Authorization': auth,
                'X-Forwarded-For': '1.2.3.4, 2.2.2.2',
                'X-Forwarded-Proto': 'http',
                'Content-Type': 'application/json',
            }, data='{}', timeout=1)
            content = await result.content.read()

        if 'hits' in json.loads(content):
            return True
        attempts += 1
        # Each call makes a new ES scroll context, which is expensive
        # and can slow the tests. If/when this isn't the case, the sleep
        # can be shortened
        await ORIGINAL_SLEEP(3)


def read_file(path):
    with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as file:
        return file.read().decode('utf-8')


async def delete_all_es_data():
    async with aiohttp.ClientSession() as session:
        await session.delete('http://127.0.0.1:9200/*')
        await session.post('http://127.0.0.1:9200/_refresh')

    await fetch_until('http://127.0.0.1:9200/_search', has_exactly(0))


async def delete_all_redis_data():
    redis_client = await aioredis.create_redis('redis://127.0.0.1:6379')
    await redis_client.execute('FLUSHDB')


async def fetch_all_es_data_until(condition):
    return await fetch_until('http://127.0.0.1:9200/activities/_search', condition)


async def fetch_es_index_names():
    async with aiohttp.ClientSession() as session:
        response = await session.get('http://127.0.0.1:9200/_alias')
        return json.loads(await response.text()).keys()


async def fetch_es_index_names_with_alias():
    async with aiohttp.ClientSession() as session:
        response = await session.get('http://127.0.0.1:9200/_alias')
        indexes = json.loads(await response.text())
    return [
        index_name
        for index_name, index_details in indexes.items()
        if index_details['aliases']
    ]


async def fetch_until(url, condition):
    async def fetch_all_es_data():
        async with aiohttp.ClientSession() as session:
            results = await session.get(url)
            return json.loads(await results.text())

    while True:
        all_es_data = await fetch_all_es_data()
        if condition(all_es_data):
            break
        await ORIGINAL_SLEEP(1)

    return all_es_data


def append_until(condition):
    future = asyncio.Future()

    all_data = []

    def append(data):
        if not future.done():
            all_data.append(data)
        if condition(all_data) and not future.done():
            future.set_result(all_data)

    return (future, append)


def hawk_auth_header(key_id, secret_key, url, method, content, content_type):
    return mohawk.Sender({
        'id': key_id,
        'key': secret_key,
        'algorithm': 'sha256',
    }, url, method, content=content, content_type=content_type).request_header


async def get(url, auth, x_forwarded_for, body):
    async with aiohttp.ClientSession() as session:
        result = await session.get(url, headers={
            'Authorization': auth,
            'Content-Type': 'application/json',
            'X-Forwarded-For': x_forwarded_for,
            'X-Forwarded-Proto': 'http',
        }, data=body, timeout=3)
        text = await result.text()
    return (text, result.status, result.headers)


async def get_until(url, x_forwarded_for, condition):
    return await get_until_with_body(url, x_forwarded_for, condition,
                                     b'{"size": 1000, "sort": [{"published": {"order": "desc"}}]}')


async def get_until_with_body(url, x_forwarded_for, condition, body):
    while True:
        auth = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url, 'GET', body, 'application/json',
        )
        all_data, status, headers = await get(url, auth, x_forwarded_for, body)
        dict_data = json.loads(all_data)
        if condition(dict_data):
            break
        await ORIGINAL_SLEEP(1)

    return dict_data, status, headers


async def get_until_raw(url, x_forwarded_for, condition):
    for _ in range(0, 180):
        auth = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url, 'GET', b'', 'application/json',
        )
        all_data, status, headers = await get(url, auth, x_forwarded_for, b'')
        passed = condition(all_data)
        await ORIGINAL_SLEEP(0 if passed else 1)
        if passed:
            break

    return all_data, status, headers


async def post(url, auth, x_forwarded_for):
    return await post_with_headers(url, {
        'Authorization': auth,
        'Content-Type': '',
        'X-Forwarded-For': x_forwarded_for,
        'X-Forwarded-Proto': 'http',
    })


async def post_with_headers(url, headers):
    async with aiohttp.ClientSession(skip_auto_headers=['Content-Type']) as session:
        result = await session.post(url, headers=headers, timeout=1)
    return (await result.text(), result.status)


async def get_with_headers(url, headers):
    async with aiohttp.ClientSession(skip_auto_headers=['Content-Type']) as session:
        result = await session.get(url, headers=headers, timeout=1, data=b'{}')
    return (await result.text(), result.status)


def respond_http(text, status):
    async def response(_):
        return web.Response(text=text, status=status, content_type='application/json')

    return response


def respond_shards(request):
    is_schema = 'schema' in request.match_info['index_name']
    shard_state = \
        [{'state': 'STARTED'}] * 1 if is_schema else \
        [{'state': 'STARTED'}] * 6
    return web.Response(text=json.dumps(shard_state), status=200, content_type='application/json')


async def run_es_application(port, override_routes):
    default_routes = [
        web.get('/_cat/nodes', respond_http('[{},{},{}]', 200)),
        web.get('/_cat/shards/{index_name}', respond_shards),
        web.put('/{index_name}/_mapping/_doc', respond_http('{}', 200)),
        web.put('/{index_name}', respond_http('{}', 200)),
        web.get('/{index_names}/_count', respond_http('{"count":0}', 200)),
        web.delete('/{index_names}', respond_http('{}', 200)),
        web.get('/activities/_search', respond_http('{"hits":{},"_scroll_id":"test"}', 200)),
        web.post('/_bulk', respond_http('{}', 200)),
        web.get('/*/_alias/{index_name}', respond_http('{}', 200)),
        web.get('/_aliases', respond_http('{}', 200)),
    ]

    routes_no_duplicates = {
        (route.method, route.path): route
        for route in (default_routes+override_routes)
    }.values()

    return await _web_application(port=port, routes=routes_no_duplicates)


async def run_feed_application(feed, status, headers, feed_requested_callback, port):
    async def handle(request):
        path = request.match_info['feed']
        asyncio.get_event_loop().call_soon(feed_requested_callback, request)
        return web.Response(text=feed(path), status=status(), headers=headers())

    routes = [web.get('/{feed}', handle), web.post('/{feed}', handle)]
    return await _web_application(port=port, routes=routes)


async def run_sentry_application():
    # Very roughly simulates a flaky Sentry endpoint
    num_calls = 0
    responses = [
        web.Response(text='', status=200, headers={}),
        web.Response(text='', status=429, headers={'Retry-After': '1'}),
        web.Response(text='', status=500, headers={'X-Sentry-Error': 'Error from Sentry'}),
    ]

    async def handle(_):
        nonlocal num_calls
        response = responses[num_calls % len(responses)]
        num_calls += 1
        return response

    routes = [web.post('/api/123/store/', handle)]
    return await _web_application(port=9872, routes=routes)


async def _web_application(port, routes):
    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', port)
    await site.start()
    return runner


def has_at_least(num_results):
    return lambda results: (
        'hits' in results and 'hits' in results['hits'] and
        len(results['hits']['hits']) >= num_results
    )


def has_exactly(num_results):
    return lambda results: (
        'hits' in results and 'hits' in results['hits'] and
        len(results['hits']['hits']) == num_results
    )


def has_at_least_hits(num_results):
    return lambda results: len(results['hits']['hits']) >= num_results


def mock_env():
    return {
        'PORT': '8080',
        'FEEDS__1__UNIQUE_ID': 'first_feed',
        'FEEDS__1__SEED': 'http://localhost:8081/tests_fixture_activity_stream_1.json',
        'FEEDS__1__ACCESS_KEY_ID': 'feed-some-id',
        'FEEDS__1__SECRET_ACCESS_KEY': '?[!@$%^%',
        'FEEDS__1__TYPE': 'activity_stream',
        'INCOMING_ACCESS_KEY_PAIRS__1__KEY_ID': 'incoming-some-id-1',
        'INCOMING_ACCESS_KEY_PAIRS__1__SECRET_KEY': 'incoming-some-secret-1',
        'INCOMING_ACCESS_KEY_PAIRS__1__PERMISSIONS__objects__1': '__MATCH_ALL__',
        'INCOMING_ACCESS_KEY_PAIRS__1__PERMISSIONS__activities__1': '__MATCH_ALL__',
        'INCOMING_ACCESS_KEY_PAIRS__2__KEY_ID': 'incoming-some-id-2',
        'INCOMING_ACCESS_KEY_PAIRS__2__SECRET_KEY': 'incoming-some-secret-2',
        'INCOMING_ACCESS_KEY_PAIRS__2__PERMISSIONS__objects__1': '__MATCH_ALL__',
        'INCOMING_ACCESS_KEY_PAIRS__2__PERMISSIONS__activities__1': '__MATCH_ALL__',
        'INCOMING_ACCESS_KEY_PAIRS__3__KEY_ID': 'incoming-some-id-3',
        'INCOMING_ACCESS_KEY_PAIRS__3__SECRET_KEY': 'incoming-some-secret-3',
        'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1': '__MATCH_ALL__',
        'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1': '__MATCH_ALL__',
        'INCOMING_IP_WHITELIST__1': '1.2.3.4',
        'INCOMING_IP_WHITELIST__2': '2.3.4.5',
        'SENTRY_DSN': 'http://abc:cvb@localhost:9872/123',
        'SENTRY_ENVIRONMENT': 'test',
        'GETADDRESS_API_KEY': 'debug',
        'GETADDRESS_API_URL': 'http://localhost:6099',
        'VCAP_SERVICES': (
            '{'
            '"redis":[{"credentials":{"uri":"redis://127.0.0.1:6379"}}],'
            '"elasticsearch":[{"credentials":{"uri":"http://some-id:some-secret@127.0.0.1:9200"}}]'
            '}'
        ),
    }
