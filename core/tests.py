import asyncio
import datetime
import json
import os
from subprocess import Popen
import sys
import unittest
from unittest.mock import Mock, patch

import aiohttp
from aiohttp import web
from freezegun import freeze_time

from core.tests_utils import (
    append_until,
    async_test,
    delete_all_es_data,
    fetch_all_es_data_until,
    fetch_es_index_names,
    get,
    get_until,
    has_at_least,
    has_at_least_ordered_items,
    hawk_auth_header,
    is_http_accepted_eventually,
    mock_env,
    post,
    post_with_headers,
    read_file,
    respond_http,
    run_app_until_accepts_http,
    run_es_application,
    run_feed_application,
    wait_until_get_working,
)

ORIGINAL_SLEEP = asyncio.sleep


class TestBase(unittest.TestCase):

    async def setup_manual(self, env, mock_feed):
        ''' Test setUp function that can be customised on a per-test basis '''

        await delete_all_es_data()

        os_environ_patcher = patch.dict(os.environ, env)
        os_environ_patcher.start()
        self.addCleanup(os_environ_patcher.stop)

        def add_async_cleanup(coroutine):
            loop = asyncio.get_event_loop()
            self.addCleanup(loop.run_until_complete, coroutine())

        self.feed_requested = [asyncio.Future(), asyncio.Future()]

        def feed_requested_callback(request):
            try:
                first_not_done = next(
                    future for future in self.feed_requested if not future.done())
            except StopIteration:
                pass
            else:
                first_not_done.set_result(request)

        feed_runner = await run_feed_application(mock_feed, feed_requested_callback, 8081)
        add_async_cleanup(feed_runner.cleanup)

        cleanup = await run_app_until_accepts_http()
        add_async_cleanup(cleanup)


class TestAuthentication(TestBase):

    @async_test
    async def test_no_auth_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        text, status = await post_with_headers(url, {
            'Content-Type': '',
            'X-Forwarded-For': '1.2.3.4, 127.0.0.0',
            'X-Forwarded-Proto': 'http',
        })
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Authentication credentials were not provided."}')

    @async_test
    async def test_bad_id_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-incorrect', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status = await post(url, auth, x_forwarded_for)
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_bad_secret_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-2', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status = await post(url, auth, x_forwarded_for)
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_bad_method_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '', 'application/json',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status = await post(url, auth, x_forwarded_for)
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_bad_content_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', 'content', '',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status = await post(url, auth, x_forwarded_for)
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_bad_content_type_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', 'some-type',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status = await post(url, auth, x_forwarded_for)
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_no_content_type_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', 'some-type',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status = await post_with_headers(url, {
            'Authorization': auth,
            'X-Forwarded-For': x_forwarded_for,
            'X-Forwarded-Proto': 'http',
        })
        self.assertEqual(status, 401)
        self.assertIn('Content-Type header was not set.', text)

    @async_test
    async def test_no_proto_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        text, status = await post_with_headers(url, {
            'Authorization': auth,
            'Content-Type': '',
            'X-Forwarded-For': '1.2.3.4, 127.0.0.0',
        })
        self.assertEqual(status, 401)
        self.assertIn('The X-Forwarded-Proto header was not set.', text)

    @async_test
    async def test_time_skew_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        past = datetime.datetime.now() + datetime.timedelta(seconds=-61)
        with freeze_time(past):
            auth = hawk_auth_header(
                'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
            )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status = await post(url, auth, x_forwarded_for)
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_repeat_auth_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        _, status_1 = await post(url, auth, x_forwarded_for)
        self.assertEqual(status_1, 200)

        text_2, status_2 = await post(url, auth, x_forwarded_for)
        self.assertEqual(status_2, 401)
        self.assertEqual(text_2, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_nonces_cleared(self):
        ''' Makes duplicate requests, but with the code patched so the nonce expiry time
            is shorter then the allowed Hawk skew. The second request succeeding gives
            evidence that the cache of nonces was cleared.
        '''

        now = datetime.datetime.now()
        past = now + datetime.timedelta(seconds=-45)

        with patch('core.app.NONCE_EXPIRE', 30):
            await self.setup_manual(env=mock_env(), mock_feed=read_file)

            url = 'http://127.0.0.1:8080/v1/'
            x_forwarded_for = '1.2.3.4, 127.0.0.0'

            with freeze_time(past):
                auth = hawk_auth_header(
                    'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
                )
                _, status_1 = await post(url, auth, x_forwarded_for)
            self.assertEqual(status_1, 200)

            with freeze_time(now):
                _, status_2 = await post(url, auth, x_forwarded_for)
            self.assertEqual(status_2, 200)

    @async_test
    async def test_no_x_fwd_for_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        text, status = await post_with_headers(url, {
            'Authorization': auth,
            'Content-Type': '',
        })
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_bad_x_fwd_for_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        x_forwarded_for = '3.4.5.6, 127.0.0.0'
        text, status = await post(url, auth, x_forwarded_for)
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_beginning_x_fwd_for_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4, 3.4.5.6, 127.0.0.0'
        text, status = await post(url, auth, x_forwarded_for)
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_too_few_x_fwd_for_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4'
        text, status = await post(url, auth, x_forwarded_for)
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_second_id_returns_object(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-2', 'incoming-some-secret-2', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status = await post(url, auth, x_forwarded_for)
        self.assertEqual(status, 200)
        self.assertEqual(text, '{"secret": "to-be-hidden"}')

    @async_test
    async def test_post_returns_object(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status = await post(url, auth, x_forwarded_for)
        self.assertEqual(status, 200)
        self.assertEqual(text, '{"secret": "to-be-hidden"}')

    @async_test
    async def test_post_creds_get_403(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '', 'application/json',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status, _ = await get(url, auth, x_forwarded_for, b'')
        self.assertEqual(status, 403)
        self.assertEqual(text, '{"details": "You are not authorized to perform this action."}')


class TestApplication(TestBase):

    @async_test
    async def test_get_returns_feed_data(self):
        url = 'http://127.0.0.1:8080/v1/'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_activity_stream_1.json'

        def read_specific_file(_):
            with open('core/' + path, 'rb') as file:
                return file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_specific_file)
            await fetch_all_es_data_until(has_at_least(2), ORIGINAL_SLEEP)

        result, status, headers = await get_until(url, x_forwarded_for,
                                                  has_at_least_ordered_items(2), asyncio.sleep)
        self.assertEqual(status, 200)
        self.assertEqual(result['orderedItems'][0]['id'],
                         'dit:exportOpportunities:Enquiry:49863:Create')
        self.assertEqual(result['orderedItems'][1]['id'],
                         'dit:exportOpportunities:Enquiry:49862:Create')
        self.assertEqual(headers['Server'], 'activity-stream')

        def does_not_have_previous_items(results):
            return '49863' not in str(results)

        path = 'tests_fixture_activity_stream_2.json'
        with patch('asyncio.sleep', wraps=fast_sleep):
            result, status, headers = await get_until(url, x_forwarded_for,
                                                      does_not_have_previous_items, asyncio.sleep)
        self.assertEqual(status, 200)
        self.assertEqual(result['orderedItems'][0]['id'],
                         'dit:exportOpportunities:Enquiry:42863:Create')
        self.assertEqual(result['orderedItems'][1]['id'],
                         'dit:exportOpportunities:Enquiry:42862:Create')
        self.assertEqual(headers['Server'], 'activity-stream')

    @async_test
    async def test_pagination(self):
        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_file)
            await fetch_all_es_data_until(has_at_least(2), ORIGINAL_SLEEP)

        url_1 = 'http://127.0.0.1:8080/v1/'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        await get_until(url_1, x_forwarded_for, has_at_least_ordered_items(2), asyncio.sleep)

        query = json.dumps({
            'size': '1',
        }).encode('utf-8')
        auth = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url_1,
            'GET', query, 'application/json',
        )
        result_1, status_1, _ = await get(url_1, auth, x_forwarded_for, query)
        result_1_json = json.loads(result_1)
        self.assertEqual(status_1, 200)
        self.assertEqual(len(result_1_json['orderedItems']), 1)
        self.assertEqual(result_1_json['orderedItems'][0]['id'],
                         'dit:exportOpportunities:Enquiry:49863:Create')
        self.assertIn('next', result_1_json)

        url_2 = result_1_json['next']
        auth_2 = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url_2, 'GET', b'', 'application/json',
        )
        result_2, status_2, _ = await get(url_2, auth_2, x_forwarded_for, b'')
        result_2_json = json.loads(result_2)
        self.assertEqual(status_2, 200)
        self.assertEqual(len(result_2_json['orderedItems']), 1)
        self.assertEqual(result_2_json['orderedItems'][0]['id'],
                         'dit:exportOpportunities:Enquiry:49862:Create')
        self.assertIn('next', result_2_json)

    @async_test
    async def test_pagination_expiry(self):
        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_file)
            await wait_until_get_working()

        url_1 = 'http://127.0.0.1:8080/v1/'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        await get_until(url_1, x_forwarded_for, has_at_least_ordered_items(2), asyncio.sleep)

        now = datetime.datetime.now()
        past = now + datetime.timedelta(seconds=-60)

        query = json.dumps({
            'size': '1',
        }).encode('utf-8')

        with freeze_time(past):
            auth = hawk_auth_header(
                'incoming-some-id-3', 'incoming-some-secret-3', url_1,
                'GET', query, 'application/json',
            )
            result_1, _, _ = await get(url_1, auth, x_forwarded_for, query)
            result_1_json = json.loads(result_1)
            url_2 = result_1_json['next']

        with freeze_time(now):
            auth_2 = hawk_auth_header(
                'incoming-some-id-3', 'incoming-some-secret-3', url_2,
                'GET', b'', 'application/json',
            )
            result_2, status_2, _ = await get(url_2, auth_2, x_forwarded_for, b'')
            self.assertEqual(json.loads(result_2)['details'], 'Scroll ID not found.')
            self.assertEqual(status_2, 404)

    @async_test
    async def test_get_can_filter(self):
        env = {
            **mock_env(),
            'FEEDS__1__SEED': (
                'http://localhost:8081/'
                'tests_fixture_activity_stream_multipage_1.json'
            ),
            'FEEDS__2__UNIQUE_ID': 'second_feed',
            'FEEDS__2__SEED': 'http://localhost:8081/tests_fixture_zendesk_1.json',
            'FEEDS__2__API_EMAIL': 'test@test.com',
            'FEEDS__2__API_KEY': 'some-key',
            'FEEDS__2__TYPE': 'zendesk',
        }

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=env, mock_feed=read_file)
            await fetch_all_es_data_until(has_at_least(4), ORIGINAL_SLEEP)

        url = 'http://127.0.0.1:8080/v1/'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        query = json.dumps({
            'query': {
                'bool': {
                    'filter': [{
                        'range': {
                            'published': {
                                'gte': '2011-04-12',
                                'lte': '2011-04-12',
                            },
                        },
                    }],
                },
            },
        }).encode('utf-8')
        auth = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url, 'GET', query, 'application/json',
        )
        result, status, _ = await get(url, auth, x_forwarded_for, query)
        self.assertEqual(status, 200)
        data = json.loads(result)
        self.assertEqual(len(data['orderedItems']), 2)
        self.assertIn('2011-04-12', data['orderedItems'][0]['published'])
        self.assertIn('2011-04-12', data['orderedItems'][1]['published'])

        query = json.dumps({
            'query': {
                'bool': {
                    'filter': [{
                        'range': {
                            'published': {
                                'gte': '2011-04-12',
                                'lte': '2011-04-12',
                            },
                        },
                    }, {
                        'term': {
                            'type': 'Create',
                        },
                    }, {
                        'term': {
                            'object.type': 'dit:exportOpportunities:Enquiry',
                        },
                    }],
                },
            },
        }).encode('utf-8')
        auth = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url, 'GET', query, 'application/json',
        )
        result, status, _ = await get(url, auth, x_forwarded_for, query)
        self.assertEqual(status, 200)
        data = json.loads(result)
        self.assertEqual(len(data['orderedItems']), 1)
        self.assertIn('2011-04-12', data['orderedItems'][0]['published'])
        self.assertEqual('Create', data['orderedItems'][0]['type'])
        self.assertIn('dit:exportOpportunities:Enquiry', data['orderedItems'][0]['object']['type'])

    @freeze_time('2012-01-14 12:00:01')
    @patch('os.urandom', return_value=b'something-random')
    @async_test
    async def test_single_page(self, _):
        posted_to_es_once, append_es = append_until(lambda results: len(results) == 1)

        async def return_200_and_callback(request):
            content, headers = (await request.content.read(), request.headers)
            asyncio.get_event_loop().call_soon(append_es, (content, headers))
            return await respond_http('{}', 200)(request)

        routes = [
            web.post('/_bulk', return_200_and_callback),
        ]
        es_runner = await run_es_application(port=9201, override_routes=routes)
        await self.setup_manual(env={**mock_env(), 'ELASTICSEARCH__PORT': '9201'},
                                mock_feed=read_file)

        [[es_bulk_content, es_bulk_headers]] = await posted_to_es_once
        es_bulk_request_dicts = [
            json.loads(line)
            for line in es_bulk_content.split(b'\n')[0:-1]
        ]

        await es_runner.cleanup()

        self.assertEqual(self.feed_requested[0].result(
        ).headers['Authorization'], (
            'Hawk '
            'mac="keUgjONtI1hLtS4DzGl+0G63o1nPFmvtIsTsZsB/NPM=", '
            'hash="B0weSUXsMcb5UhL41FZbrUJCAotzSI3HawE1NPLRUz8=", '
            'id="feed-some-id", '
            'ts="1326542401", '
            'nonce="c29tZX"'
        ))

        self.assertEqual(
            es_bulk_headers['Authorization'],
            'AWS4-HMAC-SHA256 '
            'Credential=some-id/20120114/us-east-2/es/aws4_request, '
            'SignedHeaders=content-type;host;x-amz-date, '
            'Signature=959e16e7d72ce7d29860d393381d7e752a88404c43baabfdfcb65c151fbfd625')
        self.assertEqual(es_bulk_content.decode('utf-8')[-1], '\n')
        self.assertEqual(es_bulk_headers['Content-Type'], 'application/x-ndjson')

        self.assertIn('activities_', es_bulk_request_dicts[0]['index']['_index'])
        self.assertEqual(es_bulk_request_dicts[0]['index']['_type'], '_doc')
        self.assertEqual(es_bulk_request_dicts[0]['index']
                         ['_id'], 'dit:exportOpportunities:Enquiry:49863:Create')
        self.assertEqual(es_bulk_request_dicts[1]['published'], '2018-04-12T12:48:13+00:00')
        self.assertEqual(es_bulk_request_dicts[1]['type'], 'Create')
        self.assertEqual(es_bulk_request_dicts[1]['object']
                         ['type'][1], 'dit:exportOpportunities:Enquiry')
        self.assertEqual(es_bulk_request_dicts[1]['actor']['dit:companiesHouseNumber'], '123432')

        self.assertIn('activities_', es_bulk_request_dicts[2]['index']['_index'])
        self.assertEqual(es_bulk_request_dicts[2]['index']['_type'], '_doc')
        self.assertEqual(es_bulk_request_dicts[2]['index']
                         ['_id'], 'dit:exportOpportunities:Enquiry:49862:Create')
        self.assertEqual(es_bulk_request_dicts[3]['published'], '2018-03-23T17:06:53+00:00')
        self.assertEqual(es_bulk_request_dicts[3]['type'], 'Create')
        self.assertEqual(es_bulk_request_dicts[3]['object']
                         ['type'][1], 'dit:exportOpportunities:Enquiry')
        self.assertEqual(es_bulk_request_dicts[3]['actor']['dit:companiesHouseNumber'], '82312')

    @async_test
    async def test_es_auth(self):
        get_es_once, append_es = append_until(lambda results: len(results) == 1)

        async def return_200_and_callback(request):
            content, headers = (await request.content.read(), request.headers)
            asyncio.get_event_loop().call_soon(append_es, (content, headers))
            return await respond_http('{}', 200)(request)

        routes = [
            web.get('/activities/_search', return_200_and_callback),
        ]
        es_runner = await run_es_application(port=9201, override_routes=routes)

        with \
                freeze_time('2012-01-15 12:00:01'), \
                patch('os.urandom', return_value=b'something-random'):
            await self.setup_manual(env={**mock_env(), 'ELASTICSEARCH__PORT': '9201'},
                                    mock_feed=read_file)
            await es_runner.cleanup()
            [[_, es_headers]] = await get_es_once

        self.assertEqual(es_headers['Authorization'],
                         'AWS4-HMAC-SHA256 '
                         'Credential=some-id/20120115/us-east-2/es/aws4_request, '
                         'SignedHeaders=content-type;host;x-amz-date, '
                         'Signature=210234795d8e908f10d6374144ae3ac9b49b2f84f10a8b'
                         'a189a207ae8955c505')

    @async_test
    async def test_es_401_is_proxied(self):
        routes = [
            web.get('/activities/_search', respond_http('{"elasticsearch": "error"}', 401)),
        ]
        es_runner = await run_es_application(port=9201, override_routes=routes)
        await self.setup_manual(env={**mock_env(), 'ELASTICSEARCH__PORT': '9201'},
                                mock_feed=read_file)

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url, 'GET', '', 'application/json',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status, _ = await get(url, auth, x_forwarded_for, b'')
        await es_runner.cleanup()

        self.assertEqual(status, 401)
        self.assertEqual(text, '{"elasticsearch": "error"}')

    @async_test
    async def test_es_503_recovered_from(self):
        modified = 0
        max_modifications = 2

        http_200 = b'HTTP/1.1 200 OK'
        http_500 = b'HTTP/1.1 503 Service Unavailable'

        def modify(data):
            nonlocal modified
            should_modify = modified < max_modifications and b'"status":201' in data
            data, modified = \
                (data.replace(http_200, http_500), modified + 1) if should_modify else \
                (data, modified)
            return data

        async def handle_client(local_reader, local_writer):
            try:
                remote_reader, remote_writer = await asyncio.open_connection('127.0.0.1', 9200)
                await asyncio.gather(
                    pipe(local_reader, remote_writer),  # Upstream
                    pipe(remote_reader, local_writer),  # Downstream
                )
            finally:
                local_writer.close()

        async def pipe(reader, writer):
            try:
                while not reader.at_eof():
                    writer.write(modify(await reader.read(16384)))
            finally:
                writer.close()

        server = await asyncio.start_server(handle_client, '0.0.0.0', 9201)

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env={**mock_env(), 'ELASTICSEARCH__PORT': '9201'},
                                    mock_feed=read_file)
            while modified < max_modifications:
                await ORIGINAL_SLEEP(0.1)

            await wait_until_get_working()

        url = 'http://127.0.0.1:8080/v1/'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        result, status, _ = await get_until(url, x_forwarded_for,
                                            has_at_least_ordered_items(2), asyncio.sleep)
        server.close()
        await server.wait_closed()
        self.assertEqual(status, 200)
        self.assertEqual(result['orderedItems'][0]['id'],
                         'dit:exportOpportunities:Enquiry:49863:Create')

        self.assertLessEqual(len(await fetch_es_index_names()), 2)

    @async_test
    async def test_es_no_connect_recovered(self):
        modified = 0
        max_modifications = 2

        async def handle_client(local_reader, local_writer):
            nonlocal modified
            if modified < max_modifications:
                local_writer.close()
                modified += 1
            else:
                try:
                    remote_reader, remote_writer = await asyncio.open_connection('127.0.0.1', 9200)
                    await asyncio.gather(
                        pipe(local_reader, remote_writer),  # Upstream
                        pipe(remote_reader, local_writer),  # Downstream
                    )
                finally:
                    local_writer.close()

        async def pipe(reader, writer):
            try:
                while not reader.at_eof():
                    writer.write(await reader.read(16384))
            finally:
                writer.close()

        server = await asyncio.start_server(handle_client, '0.0.0.0', 9201)

        with patch('asyncio.sleep', wraps=fast_sleep) as mock_sleep:
            await self.setup_manual(env={**mock_env(), 'ELASTICSEARCH__PORT': '9201'},
                                    mock_feed=read_file)
            while modified < max_modifications:
                await ORIGINAL_SLEEP(0.1)

            await wait_until_get_working()
            mock_sleep.assert_any_call(60)

        url = 'http://127.0.0.1:8080/v1/'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        result, status, _ = await get_until(url, x_forwarded_for,
                                            has_at_least_ordered_items(2), asyncio.sleep)
        server.close()
        await server.wait_closed()
        self.assertEqual(status, 200)
        self.assertEqual(result['orderedItems'][0]['id'],
                         'dit:exportOpportunities:Enquiry:49863:Create')

    @async_test
    async def test_es_no_connect_on_get_500(self):
        es_runner = await run_es_application(port=9201, override_routes=[])
        await self.setup_manual({
            **mock_env(),
            'ELASTICSEARCH__PORT': '9201'
        }, mock_feed=read_file)

        await es_runner.cleanup()
        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url, 'GET', '', 'application/json',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status, _ = await get(url, auth, x_forwarded_for, b'')

        self.assertEqual(status, 500)
        self.assertEqual(text, '{"details": "An unknown error occurred."}')

    @async_test
    async def test_multipage(self):
        with patch('asyncio.sleep', wraps=fast_sleep) as mock_sleep:
            await self.setup_manual(
                {**mock_env(), 'FEEDS__1__SEED': (
                    'http://localhost:8081/'
                    'tests_fixture_activity_stream_multipage_1.json'
                )
                },
                mock_feed=read_file,
            )
            results = await fetch_all_es_data_until(has_at_least(2), ORIGINAL_SLEEP)
            mock_sleep.assert_any_call(0)

        self.assertIn('dit:exportOpportunities:Enquiry:4986999:Create',
                      str(results))

    @async_test
    async def test_two_feeds(self):
        env = {
            **mock_env(),
            'FEEDS__2__UNIQUE_ID': 'second_feed',
            'FEEDS__2__SEED': 'http://localhost:8081/tests_fixture_activity_stream_2.json',
            'FEEDS__2__ACCESS_KEY_ID': 'feed-some-id',
            'FEEDS__2__SECRET_ACCESS_KEY': '?[!@$%^%',
            'FEEDS__2__TYPE': 'activity_stream',
        }

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=env, mock_feed=read_file)
            results = await fetch_all_es_data_until(has_at_least(4), ORIGINAL_SLEEP)

        self.assertIn('dit:exportOpportunities:Enquiry:49863:Create', str(results))
        self.assertIn('dit:exportOpportunities:Enquiry:42863:Create', str(results))

    @async_test
    async def test_two_feeds_one_fails(self):
        env = {
            **mock_env(),
            'FEEDS__2__UNIQUE_ID': 'failing_feed',
            'FEEDS__2__SEED': 'http://localhost:1223/',
            'FEEDS__2__ACCESS_KEY_ID': 'feed-some-id',
            'FEEDS__2__SECRET_ACCESS_KEY': '?[!@$%^%',
            'FEEDS__2__TYPE': 'activity_stream',
        }

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=env, mock_feed=read_file)
            results = await fetch_all_es_data_until(has_at_least(2), ORIGINAL_SLEEP)

        self.assertIn('dit:exportOpportunities:Enquiry:49863:Create', str(results))

    @async_test
    async def test_zendesk(self):
        def has_two_zendesk_tickets(results):
            if 'hits' not in results or 'hits' not in results['hits']:
                return False

            is_zendesk_ticket = [
                item
                for item in results['hits']['hits']
                for source in [item['_source']]
                if 'dit:application' in source and source['dit:application'] == 'zendesk'
            ]
            return len(is_zendesk_ticket) == 2

        env = {
            **mock_env(),
            'FEEDS__2__UNIQUE_ID': 'second_feed',
            'FEEDS__2__SEED': 'http://localhost:8081/tests_fixture_zendesk_1.json',
            'FEEDS__2__API_EMAIL': 'test@test.com',
            'FEEDS__2__API_KEY': 'some-key',
            'FEEDS__2__TYPE': 'zendesk',
        }

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=env, mock_feed=read_file)
            results_dict = await fetch_all_es_data_until(has_two_zendesk_tickets, ORIGINAL_SLEEP)

        results = json.dumps(results_dict)
        self.assertIn('"dit:zendesk:Ticket:1"', results)
        self.assertIn('"dit:zendesk:Ticket:1:Create"', results)
        self.assertIn('"2011-04-12T12:48:13+00:00"', results)
        self.assertIn('"dit:zendesk:Ticket:3"', results)
        self.assertIn('"dit:zendesk:Ticket:3:Create"', results)
        self.assertIn('"2011-04-12T12:48:13+00:00"', results)

    @async_test
    async def test_on_bad_json_retries(self):
        sent_broken = False

        def read_file_broken_then_fixed(path):
            nonlocal sent_broken

            feed_contents_maybe_broken = (
                read_file(path) +
                ('something-invalid' if not sent_broken else '')
            )
            sent_broken = True
            return feed_contents_maybe_broken

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_file_broken_then_fixed)
            results = await fetch_all_es_data_until(has_at_least(1), ORIGINAL_SLEEP)

        self.assertIn(
            'dit:exportOpportunities:Enquiry:49863:Create',
            str(results),
        )

    @async_test
    async def test_returns_some_metrics(self):
        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_file)
            await fetch_all_es_data_until(has_at_least(2), ORIGINAL_SLEEP)

        # Might be a bit flaky
        await ORIGINAL_SLEEP(4)
        async with aiohttp.ClientSession() as session:
            url = 'http://127.0.0.1:8080/metrics'
            result = await session.get(url)
        text = await result.text()
        self.assertIn('python_info', text)
        self.assertIn('ingest_feed_duration_seconds_count', text)
        self.assertIn('feed_unique_id="first_feed"', text)
        self.assertIn('status="success"', text)
        self.assertIn('ingest_activities_nonunique_total{feed_unique_id="first_feed"}',
                      text)
        self.assertIn('ingest_page_duration_seconds_bucket'
                      '{feed_unique_id="first_feed",le="0.005",stage="push"', text)
        self.assertIn('elasticsearch_activities_total{searchable="searchable"} 2.0', text)
        self.assertIn('elasticsearch_feed_activities_total'
                      '{feed_unique_id="first_feed",searchable="searchable"} 2.0', text)

    @async_test
    async def test_empty_feed_is_success(self):
        env = {
            **mock_env(),
            'FEEDS__1__SEED': (
                'http://localhost:8081/'
                'tests_fixture_activity_stream_empty.json'
            ),
        }

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=env, mock_feed=read_file)
            await ORIGINAL_SLEEP(2)

            async with aiohttp.ClientSession() as session:
                url = 'http://127.0.0.1:8080/metrics'
                result = await session.get(url)

        self.assertIn('status="success"', await result.text())


class TestProcess(unittest.TestCase):

    async def setup_manual(self):
        await delete_all_es_data()
        self.feed_runner_1 = await run_feed_application(read_file, Mock(), 8081)
        self.server = Popen([sys.executable, '-m', 'core.app'], env={
            **mock_env(),
            'COVERAGE_PROCESS_START': os.environ['COVERAGE_PROCESS_START'],
        })

        def add_async_cleanup(coroutine):
            loop = asyncio.get_event_loop()
            self.addCleanup(loop.run_until_complete, coroutine())

        async def tear_down():
            self.server.terminate()
            await self.feed_runner_1.cleanup()

        add_async_cleanup(tear_down)

    @async_test
    async def test_server_accepts_http(self):
        await self.setup_manual()
        self.assertTrue(await is_http_accepted_eventually())


async def fast_sleep(_):
    await ORIGINAL_SLEEP(0.5)
