import asyncio
import datetime
import json
import os
import re
import unittest
from unittest.mock import patch

import aiohttp
from aiohttp import web
import aioredis
from freezegun import freeze_time


from .tests_utils import (
    ORIGINAL_SLEEP,
    append_until,
    async_test,
    delete_all_es_data,
    delete_all_redis_data,
    fast_sleep,
    fetch_all_es_data_until,
    fetch_es_index_names,
    fetch_es_index_names_with_alias,
    get,
    get_with_headers,
    get_until,
    get_until_with_body,
    has_at_least,
    has_at_least_hits,
    hawk_auth_header,
    mock_env,
    post,
    read_file,
    respond_http,
    run_app_until_accepts_http,
    run_es_application,
    run_feed_application,
    run_sentry_application,
    wait_until_get_working,
    _web_application
)


class TestBase(unittest.TestCase):

    def add_async_cleanup(self, coroutine):
        loop = asyncio.get_event_loop()
        self.addCleanup(loop.run_until_complete, coroutine())

    async def setup_manual(self, env, mock_feed, mock_feed_status, mock_headers):
        ''' Test setUp function that can be customised on a per-test basis '''

        await delete_all_es_data()
        await delete_all_redis_data()

        os_environ_patcher = patch.dict(os.environ, env, clear=True)
        os_environ_patcher.start()
        self.addCleanup(os_environ_patcher.stop)

        self.feed_requested = [asyncio.Future(), asyncio.Future()]

        def feed_requested_callback(request):
            try:
                first_not_done = next(
                    future for future in self.feed_requested if not future.done())
            except StopIteration:
                pass
            else:
                first_not_done.set_result(request)

        feed_runner = await run_feed_application(mock_feed, mock_feed_status,
                                                 mock_headers,
                                                 feed_requested_callback, 8081)
        self.add_async_cleanup(feed_runner.cleanup)

        sentry_runner = await run_sentry_application()
        self.add_async_cleanup(sentry_runner.cleanup)

        cleanup = await run_app_until_accepts_http()
        self.add_async_cleanup(cleanup)


class TestAuthentication(TestBase):

    @async_test
    async def test_no_auth_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        text, status = await get_with_headers(url, {
            'Content-Type': '',
            'X-Forwarded-For': '1.2.3.4, 127.0.0.0',
            'X-Forwarded-Proto': 'http',
        })
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Authentication credentials were not provided."}')

    @async_test
    async def test_bad_id_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-incorrect', 'incoming-some-secret-1', url, 'GET', '{}',
            'application/json',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status, _ = await get(url, auth, x_forwarded_for, b'{}')
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_bad_secret_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-2', url, 'GET', '', '{}',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status, _ = await get(url, auth, x_forwarded_for, b'{}')
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_bad_secret_then_401_v3_activities(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v3/activities/_search'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-2', url, 'GET', '', '{}',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status, _ = await get(url, auth, x_forwarded_for, b'{}')
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_bad_secret_then_401_v3_objects(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v3/objects/_search'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-2', url, 'GET', '', '{}',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status, _ = await get(url, auth, x_forwarded_for, b'{}')
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_bad_method_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '{}', 'application/json',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status, _ = await get(url, auth, x_forwarded_for, b'{}')
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_bad_content_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', 'content',
            'application/json',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status, _ = await get(url, auth, x_forwarded_for, b'{}')
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_bad_content_type_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '', 'some-type',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status, _ = await get(url, auth, x_forwarded_for, b'')
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_no_content_type_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '', 'some-type',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status = await get_with_headers(url, {
            'Authorization': auth,
            'X-Forwarded-For': x_forwarded_for,
            'X-Forwarded-Proto': 'http',
        })
        self.assertEqual(status, 401)
        self.assertIn('Content-Type header was not set.', text)

    @async_test
    async def test_no_proto_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '{}', 'application/json',
        )
        text, status = await get_with_headers(url, {
            'Authorization': auth,
            'Content-Type': '',
            'X-Forwarded-For': '1.2.3.4, 127.0.0.0',
        })
        self.assertEqual(status, 401)
        self.assertIn('The X-Forwarded-Proto header was not set.', text)

    @async_test
    async def test_time_skew_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        past = datetime.datetime.now() + datetime.timedelta(seconds=-61)
        with freeze_time(past):
            auth = hawk_auth_header(
                'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '{}',
                'application/json',
            )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status, _ = await get(url, auth, x_forwarded_for, b'{}')
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_repeat_auth_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})
        await fetch_all_es_data_until(has_at_least(1))

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '{}', 'application/json',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        _, status_1, _ = await get(url, auth, x_forwarded_for, b'{}')
        self.assertEqual(status_1, 200)

        text_2, status_2, _ = await get(url, auth, x_forwarded_for, b'')
        self.assertEqual(status_2, 401)
        self.assertEqual(text_2, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_invalid_header_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '{}', 'application/json',
        )
        bad_auth = re.sub(r'Hawk ', 'Something ', auth)
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        _, status_1, _ = await get(url, bad_auth, x_forwarded_for, b'{}')
        self.assertEqual(status_1, 401)

    @async_test
    async def test_invalid_ts_format_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '{}', 'application/json',
        )
        bad_auth = re.sub(r'ts="[^"]+"', 'ts="non-numeric"', auth)
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        _, status_1, _ = await get(url, bad_auth, x_forwarded_for, b'{}')
        self.assertEqual(status_1, 401)

    @async_test
    async def test_missing_nonce_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '{}', 'application/json',
        )
        bad_auth = re.sub(r', nonce="[^"]+"', '', auth)
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        _, status_1, _ = await get(url, bad_auth, x_forwarded_for, b'{}')
        self.assertEqual(status_1, 401)

    @async_test
    async def test_reused_nonce_then_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file,
                                mock_feed_status=lambda: 200, mock_headers=lambda: {})
        await fetch_all_es_data_until(has_at_least(1))

        url = 'http://127.0.0.1:8080/v2/activities'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '{}', 'application/json',
        )

        _, status_1, _ = await get(url, auth, x_forwarded_for, b'{}')
        self.assertEqual(status_1, 200)

        await asyncio.sleep(1)

        _, status_2, _ = await get(url, auth, x_forwarded_for, b'{}')
        self.assertEqual(status_2, 401)

    @async_test
    async def test_nonces_cleared(self):
        ''' Makes duplicate requests, but with the code patched so the nonce expiry time
            is shorter then the allowed Hawk skew. The second request succeeding gives
            evidence that the cache of nonces was cleared.
        '''
        with patch('core.app.app_incoming.NONCE_EXPIRE', 1):
            await self.setup_manual(env=mock_env(), mock_feed=read_file,
                                    mock_feed_status=lambda: 200, mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(1))

        url = 'http://127.0.0.1:8080/v2/activities'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '{}', 'application/json',
        )

        _, status_1, _ = await get(url, auth, x_forwarded_for, b'{}')
        self.assertEqual(status_1, 200)

        await asyncio.sleep(2)

        _, status_2, _ = await get(url, auth, x_forwarded_for, b'{}')
        self.assertEqual(status_2, 200)

    @async_test
    async def test_no_x_fwd_for_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file,
                                mock_feed_status=lambda: 200, mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '{}', 'application/json',
        )
        text, status = await get_with_headers(url, {
            'Authorization': auth,
            'Content-Type': '',
        })
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_bad_x_fwd_for_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '{}', 'application/json',
        )
        x_forwarded_for = '3.4.5.6, 127.0.0.0'
        text, status, _ = await get(url, auth, x_forwarded_for, b'{}')
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_beginning_x_fwd_for_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '{}', 'application/json',
        )
        x_forwarded_for = '1.2.3.4, 3.4.5.6, 127.0.0.0'
        text, status, _ = await get(url, auth, x_forwarded_for, b'{}')
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_too_few_x_fwd_for_401(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '{}', 'application/json',
        )
        x_forwarded_for = '1.2.3.4'
        text, status, _ = await get(url, auth, x_forwarded_for, b'{}')
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    @async_test
    async def test_post_creds_get_405(self):
        await self.setup_manual(env=mock_env(), mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', 'application/json',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status = await post(url, auth, x_forwarded_for, b'')
        self.assertEqual(status, 405)
        self.assertEqual(text, '{"details": "405: Method Not Allowed"}')

    @async_test
    async def test_ip_within_subnet_gets_200(self):
        mock_env_with_whitelist = mock_env()
        mock_env_with_whitelist['INCOMING_IP_WHITELIST__3'] = '10.0.0.0/8'
        url = 'http://127.0.0.1:8080/v2/activities'
        x_forwarded_for = '10.1.1.1, 127.0.0.0'

        path = 'tests_fixture_activity_stream_1.json'

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as file:
                return file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env_with_whitelist, mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(2))

        _, status, _ = await get_until(url, x_forwarded_for,
                                       has_at_least_hits(2))
        self.assertEqual(status, 200)

    @async_test
    async def test_ip_outside_subnet_gets_401(self):
        mock_env_with_whitelist = mock_env()
        mock_env_with_whitelist['INCOMING_IP_WHITELIST__3'] = '10.0.0.0/8'
        await self.setup_manual(env=mock_env_with_whitelist, mock_feed=read_file,
                                mock_feed_status=lambda: 200, mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        x_forwarded_for = '9.1.1.1, 127.0.0.0'

        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '{}', 'application/json',
        )
        text, status, _ = await get(url, auth, x_forwarded_for, b'')
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')


class TestApplication(TestBase):

    @async_test
    async def test_get_returns_feed_data(self):
        url = 'http://127.0.0.1:8080/v2/activities'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_activity_stream_1.json'

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as file:
                return file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(2))

        result, status, headers = await get_until(url, x_forwarded_for,
                                                  has_at_least_hits(2))
        self.assertEqual(status, 200)
        ids = [item['_source']['id'] for item in result['hits']['hits']]
        self.assertIn('dit:exportOpportunities:Enquiry:49863:Create', ids)
        self.assertIn('dit:exportOpportunities:Enquiry:49862:Create', ids)
        self.assertEqual(headers['Server'], 'activity-stream')

        def does_not_have_previous_items(results):
            return '49863' not in str(results)

        path = 'tests_fixture_activity_stream_2.json'
        with patch('asyncio.sleep', wraps=fast_sleep):
            result, status, headers = await get_until(url, x_forwarded_for,
                                                      does_not_have_previous_items)
        self.assertEqual(status, 200)
        ids = [item['_source']['id'] for item in result['hits']['hits']]
        self.assertIn('dit:exportOpportunities:Enquiry:42863:Create', ids)
        self.assertIn('dit:exportOpportunities:Enquiry:42862:Create', ids)
        self.assertEqual(headers['Server'], 'activity-stream')

    @async_test
    async def test_v2_activities_get_returns_feed_data(self):
        url = 'http://127.0.0.1:8080/v2/activities'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_activity_stream_1.json'

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as file:
                return file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(2))

        result, status, _ = await get_until(url, x_forwarded_for,
                                            has_at_least(2))
        self.assertEqual(status, 200)
        ids = [item['_source']['id'] for item in result['hits']['hits']]
        self.assertIn('dit:exportOpportunities:Enquiry:49863:Create', ids)
        self.assertIn('dit:exportOpportunities:Enquiry:49862:Create', ids)

        def does_not_have_previous_items(results):
            return '49863' not in str(results)

        path = 'tests_fixture_activity_stream_2.json'
        with patch('asyncio.sleep', wraps=fast_sleep):
            result, status, _ = await get_until(url, x_forwarded_for,
                                                does_not_have_previous_items)
        self.assertEqual(status, 200)
        ids = [item['_source']['id'] for item in result['hits']['hits']]
        self.assertIn('dit:exportOpportunities:Enquiry:42863:Create', ids)
        self.assertIn('dit:exportOpportunities:Enquiry:42862:Create', ids)

    @async_test
    async def test_v2_activities_get_term_query(self):
        url = 'http://127.0.0.1:8080/v2/objects'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_activity_stream_1.json'

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as file:
                return file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(2))

        desired_id = 'dit:exportOpportunities:Enquiry:49863'
        result, status, _ = await get_until_with_body(
            url, x_forwarded_for, has_at_least(1), json.dumps({
                'query': {'term': {'id': desired_id}}
            }).encode('utf-8'))

        self.assertEqual(status, 200)
        ids = [item['_source']['id'] for item in result['hits']['hits']]
        self.assertEqual([desired_id], ids)

    @async_test
    async def test_v3_objects_get_term_query(self):
        url = 'http://127.0.0.1:8080/v3/objects/_search'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_activity_stream_1.json'

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as file:
                return file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(2))

        desired_id = 'dit:exportOpportunities:Enquiry:49863'
        body = json.dumps({
            'query': {'term': {'id': desired_id}}
        }).encode('utf-8')
        result, status, _ = await get_until_with_body(url,
                                                      x_forwarded_for, has_at_least(1), body)

        self.assertEqual(status, 200)
        ids = [item['_source']['id'] for item in result['hits']['hits']]
        self.assertEqual([desired_id], ids)

        auth = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url, 'POST', body, 'application/json',
        )
        result_raw, status = await post(url, auth, x_forwarded_for, body)
        result = json.loads(result_raw)
        self.assertEqual(status, 200)
        ids = [item['_source']['id'] for item in result['hits']['hits']]
        self.assertEqual([desired_id], ids)

    @async_test
    async def test_v2_activities_get_bool_query(self):
        url = 'http://127.0.0.1:8080/v2/objects'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_activity_stream_1.json'

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as file:
                return file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(2))

        desired_id = 'dit:exportOpportunities:Enquiry:49863'
        result, status, _ = await get_until_with_body(
            url, x_forwarded_for, has_at_least(1), json.dumps({
                'query': {'bool': {'must': {'term': {'id': desired_id}}}}
            }).encode('utf-8'))

        self.assertEqual(status, 200)
        ids = [item['_source']['id'] for item in result['hits']['hits']]
        self.assertEqual([desired_id], ids)

    @async_test
    async def test_v2_activities_get_function_score_query(self):
        url = 'http://127.0.0.1:8080/v2/objects'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_activity_stream_1.json'

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as file:
                return file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(2))

        desired_id = 'dit:exportOpportunities:Enquiry:49863'
        result, status, _ = await get_until_with_body(
            url, x_forwarded_for, has_at_least(1), json.dumps({
                'query': {
                    'function_score': {
                        'query':  {'term': {'id': desired_id}},
                    }
                }
            }).encode('utf-8'))

        self.assertEqual(status, 200)
        ids = [item['_source']['id'] for item in result['hits']['hits']]
        self.assertEqual([desired_id], ids)

    @async_test
    async def test_v2_activities_get_filter_obj_query(self):
        url = 'http://127.0.0.1:8080/v2/objects'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_activity_stream_1.json'

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as file:
                return file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(2))

        desired_id = 'dit:exportOpportunities:Enquiry:49863'
        result, status, _ = await get_until_with_body(
            url, x_forwarded_for, has_at_least(1), json.dumps({
                'query': {
                    'bool': {
                        'filter': {'term': {'id': desired_id}},
                    }
                }
            }).encode('utf-8'))

        self.assertEqual(status, 200)
        ids = [item['_source']['id'] for item in result['hits']['hits']]
        self.assertEqual([desired_id], ids)

    @async_test
    async def test_v2_activities_get_filter_list_query(self):
        url = 'http://127.0.0.1:8080/v2/objects'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_activity_stream_1.json'

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as file:
                return file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(2))

        desired_id = 'dit:exportOpportunities:Enquiry:49863'
        result, status, _ = await get_until_with_body(
            url, x_forwarded_for, has_at_least(1), json.dumps({
                'query': {
                    'bool': {
                        'filter': [{'term': {'id': desired_id}}],
                    }
                }
            }).encode('utf-8'))

        self.assertEqual(status, 200)
        ids = [item['_source']['id'] for item in result['hits']['hits']]
        self.assertEqual([desired_id], ids)

    @async_test
    async def test_v2_activities_get_aggs_query(self):
        url = 'http://127.0.0.1:8080/v2/activities'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_activity_stream_1.json'

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as file:
                return file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(2))

        result, status, _ = await get_until_with_body(
            url, x_forwarded_for, has_at_least(2), json.dumps({
                'aggs': {
                    'my_agg': {
                        'terms': {'field': 'published', 'size': 3},
                    }
                },
            }).encode('utf-8'))

        self.assertEqual(status, 200)
        self.assertEqual(len(result['hits']['hits']), 2)
        self.assertEqual(len(result['aggregations']['my_agg']['buckets']), 2)

    @async_test
    async def test_v3_activities_get_aggs_query(self):
        url = 'http://127.0.0.1:8080/v3/activities/_search'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_activity_stream_1.json'

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as file:
                return file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(2))

        result, status, _ = await get_until_with_body(
            url, x_forwarded_for, has_at_least(2), json.dumps({
                'aggs': {
                    'my_agg': {
                        'terms': {'field': 'published', 'size': 3},
                    }
                },
            }).encode('utf-8'))

        self.assertEqual(status, 200)
        self.assertEqual(len(result['hits']['hits']), 2)
        self.assertEqual(len(result['aggregations']['my_agg']['buckets']), 2)

    @async_test
    async def test_v2_activities_get_filter_aggs_query(self):
        url = 'http://127.0.0.1:8080/v2/activities'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_activity_stream_1.json'

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as file:
                return file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(2))

        result, status, _ = await get_until_with_body(
            url, x_forwarded_for, has_at_least(0), json.dumps({
                'query': {
                    'bool': {
                        'filter': {'term': {'id': 'does-not-exist'}},
                    }
                },
                'aggs': {
                    'my_agg': {
                        'terms': {'field': 'published', 'size': 3}
                    }
                },
            }).encode('utf-8'))

        self.assertEqual(status, 200)
        self.assertEqual(len(result['hits']['hits']), 0)
        self.assertEqual(result['aggregations']['my_agg']['buckets'], [])

    @async_test
    async def test_v2_activities_permissioned_empty_query(self):
        url = 'http://127.0.0.1:8080/v2/activities'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_permissions_1.json'
        env = {
            'FEEDS__1__SEED': 'http://localhost:8081/' + path,
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1__TERMS_KEY':
                'object.type',
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1__TERMS_VALUES__1':
                'object-type-b',
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1__TERMS_VALUES__2':
                'object-type-c',
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1': '__MATCH_NONE__',
            **{key: value for key, value in mock_env().items() if key not in [
                'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1',
                'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1']},
        }

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as _file:
                return _file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=env, mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(3))

        result, status, _ = await get_until_with_body(
            url, x_forwarded_for, has_at_least(1), json.dumps({
            }).encode('utf-8'))

        self.assertEqual(status, 200)
        self.assertEqual(len(result['hits']['hits']), 2)

    @async_test
    async def test_v2_activities_permissioned_filtered_query(self):
        url = 'http://127.0.0.1:8080/v2/activities'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_permissions_1.json'
        env = {
            'FEEDS__1__SEED': 'http://localhost:8081/' + path,
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1__TERMS_KEY':
                'object.type',
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1__TERMS_VALUES__1':
                'object-type-b',
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1__TERMS_VALUES__2':
                'object-type-c',
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1': '__MATCH_NONE__',
            **{key: value for key, value in mock_env().items() if key not in [
                'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1',
                'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1']},
        }

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as _file:
                return _file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=env, mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(3))

        result, status, _ = await get_until_with_body(
            url, x_forwarded_for, has_at_least(1), json.dumps({
                'query': {
                    'bool': {
                        'filter': {'term': {'object.type': 'object-type-b'}},
                    }
                },
            }).encode('utf-8'))

        self.assertEqual(status, 200)
        self.assertEqual(len(result['hits']['hits']), 1)

    @async_test
    async def test_v2_activities_permissioned_empty_query_match_none(self):
        url = 'http://127.0.0.1:8080/v2/activities'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_permissions_1.json'
        env = {
            'FEEDS__1__SEED': 'http://localhost:8081/' + path,
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1': '__MATCH_NONE__',
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1': '__MATCH_NONE__',
            **{key: value for key, value in mock_env().items() if key not in [
                'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1',
                'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1']},
        }

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as _file:
                return _file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=env, mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(3))

        result, status, _ = await get_until_with_body(
            url, x_forwarded_for, has_at_least(0), json.dumps({
            }).encode('utf-8'))

        self.assertEqual(status, 200)
        self.assertEqual(len(result['hits']['hits']), 0)

    @async_test
    async def test_v2_objects_permissioned_empty_query(self):
        url = 'http://127.0.0.1:8080/v2/objects'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_permissions_1.json'
        env = {
            'FEEDS__1__SEED': 'http://localhost:8081/' + path,
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1': '__MATCH_NONE__',
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1__TERMS_KEY': 'type',
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1__TERMS_VALUES__1':
                'object-type-b',
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1__TERMS_VALUES__2':
                'object-type-c',
            **{key: value for key, value in mock_env().items() if key not in [
                'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1',
                'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1']},
        }

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as _file:
                return _file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=env, mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(3))

        result, status, _ = await get_until_with_body(
            url, x_forwarded_for, has_at_least(1), json.dumps({
            }).encode('utf-8'))

        self.assertEqual(status, 200)
        self.assertEqual(len(result['hits']['hits']), 2)

    @async_test
    async def test_v2_objects_permissioned_filtered_query(self):
        url = 'http://127.0.0.1:8080/v2/objects'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_permissions_1.json'
        env = {
            'FEEDS__1__SEED': 'http://localhost:8081/' + path,
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1': '__MATCH_NONE__',
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1__TERMS_KEY': 'type',
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1__TERMS_VALUES__1':
                'object-type-b',
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1__TERMS_VALUES__2':
                'object-type-c',
            **{key: value for key, value in mock_env().items() if key not in [
                'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1',
                'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1']},
        }

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as _file:
                return _file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=env, mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(3))

        result, status, _ = await get_until_with_body(
            url, x_forwarded_for, has_at_least(1), json.dumps({
                'query': {
                    'bool': {
                        'filter': {'term': {'type': 'object-type-b'}},
                    }
                },
            }).encode('utf-8'))

        self.assertEqual(status, 200)
        self.assertEqual(len(result['hits']['hits']), 1)

    @async_test
    async def test_v2_objects_permissioned_empty_query_match_none(self):
        url = 'http://127.0.0.1:8080/v2/objects'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_permissions_1.json'
        env = {
            'FEEDS__1__SEED': 'http://localhost:8081/' + path,
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1': '__MATCH_NONE__',
            'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1': '__MATCH_NONE__',
            **{key: value for key, value in mock_env().items() if key not in [
                'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__activities__1',
                'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__objects__1']},
        }

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as _file:
                return _file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=env, mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(3))

        result, status, _ = await get_until_with_body(
            url, x_forwarded_for, has_at_least(0), json.dumps({
            }).encode('utf-8'))

        self.assertEqual(status, 200)
        self.assertEqual(len(result['hits']['hits']), 0)

    @async_test
    async def test_v2_objects_get_returns_feed_data(self):
        url = 'http://127.0.0.1:8080/v2/objects'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        path = 'tests_fixture_activity_stream_1.json'

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) + '/' + path, 'rb') as file:
                return file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(2))

        result, status, _ = await get_until(url, x_forwarded_for,
                                            has_at_least(2))
        self.assertEqual(status, 200)
        ids = [item['_source']['id'] for item in result['hits']['hits']]
        self.assertIn('dit:exportOpportunities:Enquiry:49863', ids)
        self.assertIn('dit:exportOpportunities:Enquiry:49862', ids)

        def does_not_have_previous_items(results):
            return '49863' not in str(results)

        path = 'tests_fixture_activity_stream_2.json'
        with patch('asyncio.sleep', wraps=fast_sleep):
            result, status, _ = await get_until(url, x_forwarded_for,
                                                does_not_have_previous_items)
        self.assertEqual(status, 200)
        ids = [item['_source']['id'] for item in result['hits']['hits']]
        self.assertIn('dit:exportOpportunities:Enquiry:42863', ids)
        self.assertIn('dit:exportOpportunities:Enquiry:42862', ids)

    @async_test
    async def test_pagination(self):
        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_file,
                                    mock_feed_status=lambda: 200, mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(2))

        url_1 = 'http://127.0.0.1:8080/v2/activities'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        await get_until(url_1, x_forwarded_for, has_at_least_hits(2))

        query = json.dumps({
            'size': '1',
            'sort': [
                {'published': {'order': 'desc'}},
            ]
        }).encode('utf-8')
        auth = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url_1,
            'GET', query, 'application/json',
        )
        result_1, status_1, _ = await get(url_1, auth, x_forwarded_for, query)
        result_1_json = json.loads(result_1)
        self.assertEqual(status_1, 200)
        self.assertEqual(len(result_1_json['hits']['hits']), 1)
        self.assertEqual(result_1_json['hits']['hits'][0]['_source']['id'],
                         'dit:exportOpportunities:Enquiry:49863:Create')
        search_after = result_1_json['hits']['hits'][0]['sort']

        query = json.dumps({
            'size': '1',
            'sort': [
                {'published': {'order': 'desc'}},
            ],
            'search_after': search_after,
        }).encode('utf-8')
        auth_2 = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url_1,
            'GET', query, 'application/json',
        )
        result_2, status_2, _ = await get(url_1, auth_2, x_forwarded_for, query)
        result_2_json = json.loads(result_2)
        self.assertEqual(status_2, 200)
        self.assertEqual(len(result_2_json['hits']['hits']), 1)
        self.assertEqual(result_2_json['hits']['hits'][0]['_source']['id'],
                         'dit:exportOpportunities:Enquiry:49862:Create')

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
            await self.setup_manual(env=env, mock_feed=read_file, mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(4))

        url = 'http://127.0.0.1:8080/v2/activities'
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
        self.assertEqual(len(data['hits']['hits']), 2)
        self.assertIn('2011-04-12', data['hits']['hits'][0]['_source']['published'])
        self.assertIn('2011-04-12', data['hits']['hits'][1]['_source']['published'])

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
        self.assertEqual(len(data['hits']['hits']), 1)
        self.assertIn('2011-04-12', data['hits']['hits'][0]['_source']['published'])
        self.assertEqual('Create', data['hits']['hits'][0]['_source']['type'])
        self.assertIn('dit:exportOpportunities:Enquiry',
                      data['hits']['hits'][0]['_source']['object']['type'])

    @freeze_time('2012-01-14 12:00:01')
    @patch('os.urandom', return_value=b'something-random')
    @patch('secrets.choice', return_value='qwerty12')
    @async_test
    async def test_single_page(self, _, __):
        posted_to_es_once, append_es = append_until(lambda results: len(results) == 1)

        async def return_200_and_callback(request):
            content, headers = (await request.content.read(), request.headers)
            asyncio.get_event_loop().call_soon(append_es, (content, headers))
            return await respond_http('{}', 200)(request)

        routes = [
            web.post('/_bulk', return_200_and_callback),
        ]

        es_runner = await run_es_application(port=9208, override_routes=routes)
        original_env = mock_env()
        vcap_services = original_env['VCAP_SERVICES'].replace(':9200', ':9208')
        self.add_async_cleanup(es_runner.cleanup)
        await self.setup_manual(env={**original_env, 'VCAP_SERVICES': vcap_services},
                                mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        [[es_bulk_content, es_bulk_headers]] = await posted_to_es_once
        es_bulk_request_dicts = [
            json.loads(line)
            for line in es_bulk_content.split(b'\n')[0:-1]
        ]

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
            'Basic c29tZS1pZDpzb21lLXNlY3JldA==')
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

    @freeze_time('2012-01-14 12:00:01')
    @patch('os.urandom', return_value=b'something-random')
    @patch('secrets.choice', return_value='qwerty12')
    @async_test
    async def test_single_page_aws_authentication(self, _, __):
        posted_to_es_once, append_es = append_until(lambda results: len(results) == 1)

        async def return_200_and_callback(request):
            content, headers = (await request.content.read(), request.headers)
            asyncio.get_event_loop().call_soon(append_es, (content, headers))
            return await respond_http('{}', 200)(request)

        routes = [
            web.post('/_bulk', return_200_and_callback),
        ]

        es_runner = await run_es_application(port=9208, override_routes=routes)
        aws_es_env = {
            **mock_env(),
            'ES_URI': 'http://127.0.0.1:9208',
            'ES_VERSION': '7.x',
            'ES_AWS_ACCESS_KEY_ID': 'AKIAIOSFODNN7EXAMPLE',
            'ES_AWS_SECRET_ACCESS_KEY': 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY',
            'ES_AWS_REGION': 'us-east-1',
        }
        self.add_async_cleanup(es_runner.cleanup)
        await self.setup_manual(env=aws_es_env,
                                mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        [[es_bulk_content, es_bulk_headers]] = await posted_to_es_once
        es_bulk_request_dicts = [
            json.loads(line)
            for line in es_bulk_content.split(b'\n')[0:-1]
        ]

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
            'AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20120114/us-east-1/es/aws4_request, '
            'SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, '
            'Signature=ae4f4eb6bdbd4382a6dd4589c67dbc000cff11859b4dd1a1632be9e227bb910b')
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

    # Performs search to /v2/objects and expects JSON response in format
    # [
    #   {
    #     heading: String
    #     title: String
    #     link: String
    #     introduction: String
    #   }, ...
    # ]
    #
    @async_test
    async def test_get_search(self):
        # -- Setup --
        # Source feed has 5 objects;
        # read from this and put them in Elasticsearch

        source_feed = 'tests_fixture_activity_stream_3.json'

        def read_specific_file(_):
            with open(os.path.dirname(os.path.abspath(__file__)) +
                      '/' + source_feed, 'rb') as file:
                return file.read().decode('utf-8')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_specific_file,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(2))

        # -- Helpers --

        # Performs authorised GET request to given URL
        async def _get(url, body):
            url = 'http://127.0.0.1:8080' + url
            x_forwarded_for = '1.2.3.4, 127.0.0.0'
            auth = hawk_auth_header(
                'incoming-some-id-3', 'incoming-some-secret-3', url,
                'GET', body, 'application/json',
            )

            result, status, headers = await get(url, auth, x_forwarded_for, body)
            return {'status': status, 'result': result, 'headers': headers}

        # -- Test --

        # Perform a search for "Article"
        body = json.dumps({
            'query': {
                'multi_match': {
                    'query': 'Article',
                    'fields': ['name',
                               'content']
                }
            },
            '_source': ['name',
                        'content',
                        'url']
        })

        response = await _get('/v2/objects', body)

        # Response has: status: 200, type: application/json, 5 results
        # First results has: header, title, url and introduction
        self.assertEqual(200, response['status'])
        self.assertEqual('application/json; charset=utf-8',
                         response['headers']['Content-Type'])
        results = json.loads(response['result'])['hits']['hits']
        self.assertEqual(5, len(results))
        article_1 = next(
            result for result in results if result['_source']['content'] == 'Article title 1'
        )
        self.assertEqual(article_1['_source'], {
            'name': 'Advice',
            'content': 'Article title 1',
            'url': 'www.great.gov.uk/article',
        })

    @async_test
    async def test_es_auth(self):
        get_es_once, append_es = append_until(lambda results: len(results) == 1)

        async def return_200_and_callback(request):
            content, headers = (await request.content.read(), request.headers)
            query =  \
                b'{"query": {"bool": {"filter": [{"match_all": {}}], ' + \
                b'"must": [{"match_all": {}}]}}}'
            if content == query:
                asyncio.get_event_loop().call_soon(append_es, (content, headers))
            return await respond_http('{"hits":{},"_scroll_id":{}}', 200)(request)

        routes = [
            web.get('/activities/_search', return_200_and_callback),
        ]
        es_runner = await run_es_application(port=9204, override_routes=routes)
        self.add_async_cleanup(es_runner.cleanup)

        original_env = mock_env()
        vcap_services = original_env['VCAP_SERVICES'].replace(':9200', ':9204')
        await self.setup_manual(env={**original_env, 'VCAP_SERVICES': vcap_services},
                                mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url, 'GET', '{}',
            'application/json',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        await get(url, auth, x_forwarded_for, b'{}')
        [[_, es_headers]] = await get_es_once

        self.assertEqual(es_headers['Authorization'], 'Basic c29tZS1pZDpzb21lLXNlY3JldA==')

    @async_test
    async def test_es_401_is_proxied(self):
        routes = [
            web.get('/activities/_search', respond_http('{"elasticsearch": "error"}', 401)),
        ]
        es_runner = await run_es_application(port=9202, override_routes=routes)

        original_env = mock_env()
        vcap_services = original_env['VCAP_SERVICES'].replace(':9200', ':9202')
        await self.setup_manual(env={**original_env, 'VCAP_SERVICES': vcap_services},
                                mock_feed=read_file, mock_feed_status=lambda: 200,
                                mock_headers=lambda: {})

        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url, 'GET', '{}', 'application/json',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status, _ = await get(url, auth, x_forwarded_for, b'{}')
        await es_runner.cleanup()

        self.assertEqual(status, 401)
        self.assertEqual(text, '{"elasticsearch": "error"}')

    @async_test
    async def test_es_5xx_recovered_from(self):
        modified_500 = 0
        modified_503 = 0
        max_modifications = 10

        http_200 = b'HTTP/1.1 200 OK'
        http_500 = b'HTTP/1.1 500 Service Unavailable'
        http_503 = b'HTTP/1.1 503 Internal Server Error'

        def modify(data):
            nonlocal modified_500
            nonlocal modified_503
            should_modify_500 = modified_500 < max_modifications and \
                (b'"status":201' in data or b'"count"' in data)
            should_modify_503 = modified_503 < max_modifications and \
                (b'"status":201' in data or b'"count"' in data)
            with_500 = data.replace(http_200, http_500)
            with_503 = data.replace(http_200, http_503)
            data, modified_500, modified_503 = \
                (with_500, modified_500 + 1, modified_503) if should_modify_500 else \
                (with_503, modified_500, modified_503 + 1) if should_modify_503 else \
                (data, modified_500, modified_503)
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

        server = await asyncio.start_server(handle_client, '0.0.0.0', 9203)
        original_env = mock_env()
        vcap_services = original_env['VCAP_SERVICES'].replace(':9200', ':9203')

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env={**original_env, 'VCAP_SERVICES': vcap_services},
                                    mock_feed=read_file, mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            while modified_500 < max_modifications or modified_503 < max_modifications:
                await ORIGINAL_SLEEP(1)

            await wait_until_get_working()

            url = 'http://127.0.0.1:8080/v2/activities'
            x_forwarded_for = '1.2.3.4, 127.0.0.0'
            result, status, _ = await get_until(url, x_forwarded_for,
                                                has_at_least_hits(2))
            server.close()
            await server.wait_closed()
            self.assertEqual(status, 200)
            self.assertEqual(set(item['_source']['id'] for item in result['hits']['hits']), {
                'dit:exportOpportunities:Enquiry:49862:Create',
                'dit:exportOpportunities:Enquiry:49863:Create',
            })

            self.assertLessEqual(len(await fetch_es_index_names_with_alias()), 4)
            await ORIGINAL_SLEEP(2)
            self.assertLessEqual(len(await fetch_es_index_names()), 9)

        async with aiohttp.ClientSession() as session:
            metrics_result = await session.get('http://127.0.0.1:8080/metrics')
            metrics_text = await metrics_result.text()
            self.assertIn('status="success"', metrics_text)

    @async_test
    async def test_delete_fail_not_snapshot(self):
        http_400 = b'HTTP/1.1 400 OK\r\ncontent-type: application/json; charset=UTF-8' \
                   b'\r\ncontent-length: 30\r\n\r\n' \
                   b'{"error":"Something horrible"}'

        previous_data = {}
        on_delete_fail = False

        def modify(data, direction, client_id):
            previous_data[(direction, client_id)] = data
            be_400 = on_delete_fail and direction == 'response' and b'DELETE' in previous_data[(
                'request', client_id)]
            return (
                http_400 if be_400 else
                data
            )

        client_id = 0

        async def handle_client(local_reader, local_writer):
            nonlocal client_id
            local_client_id = client_id
            try:
                remote_reader, remote_writer = await asyncio.open_connection('127.0.0.1', 9200)
                await asyncio.gather(
                    pipe(local_reader, remote_writer, 'request', local_client_id),  # Upstream
                    pipe(remote_reader, local_writer, 'response', local_client_id),  # Downstream
                )
            finally:
                local_writer.close()

        async def pipe(reader, writer, direction, client_id):
            try:
                while not reader.at_eof():
                    writer.write(modify(await reader.read(16384), direction, client_id))
            finally:
                writer.close()

        server = await asyncio.start_server(handle_client, '0.0.0.0', 9201)
        original_env = mock_env()
        vcap_services = original_env['VCAP_SERVICES'].replace(':9200', ':9201')

        with patch('asyncio.sleep', wraps=fast_sleep), patch('raven.Client') as raven_client:
            await self.setup_manual(env={**original_env, 'VCAP_SERVICES': vcap_services},
                                    mock_feed=read_file, mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await wait_until_get_working()
            url = 'http://127.0.0.1:8080/v2/activities'
            x_forwarded_for = '1.2.3.4, 127.0.0.0'

            await get_until(url, x_forwarded_for, has_at_least_hits(2))

            raven_client().captureException.assert_not_called()

            # The full ingest has a minimum duration, and the delete is only called at the
            # beginning of the second ingest
            on_delete_fail = True
            await ORIGINAL_SLEEP(40)

            # This is the point of the test: the exception should have been
            # captured, since this error is not expected
            raven_client().captureException.assert_called()

            server.close()
            await server.wait_closed()

    @async_test
    async def test_es_no_connect_recovered(self):
        modified = 0
        max_modifications = 4

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

        server = await asyncio.start_server(handle_client, '0.0.0.0', 9207)
        original_env = mock_env()
        vcap_services = original_env['VCAP_SERVICES'].replace(':9200', ':9207')

        with patch('asyncio.sleep', wraps=fast_sleep) as mock_sleep:
            await self.setup_manual(env={**original_env, 'VCAP_SERVICES': vcap_services},
                                    mock_feed=read_file, mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await wait_until_get_working()
            await ORIGINAL_SLEEP(3)

        url = 'http://127.0.0.1:8080/v2/activities'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        result, status, _ = await get_until(url, x_forwarded_for,
                                            has_at_least_hits(2))
        mock_sleep.assert_any_call(2)
        server.close()
        await server.wait_closed()
        self.assertEqual(status, 200)
        self.assertEqual(result['hits']['hits'][0]['_source']['id'],
                         'dit:exportOpportunities:Enquiry:49863:Create')

    @async_test
    async def test_es_no_connect_on_get_500(self):
        es_runner = await run_es_application(port=9206, override_routes=[])
        original_env = mock_env()
        vcap_services = original_env['VCAP_SERVICES'].replace(':9200', ':9206')
        env = {
            **original_env,
            'VCAP_SERVICES': vcap_services,
        }
        await self.setup_manual(
            env, mock_feed=read_file,
            mock_feed_status=lambda: 200,
            mock_headers=lambda: {},
        )

        await es_runner.cleanup()
        url = 'http://127.0.0.1:8080/v2/activities'
        auth = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url, 'GET', '{}', 'application/json',
        )
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        text, status, _ = await get(url, auth, x_forwarded_for, b'{}')

        self.assertEqual(status, 500)
        self.assertEqual(text, '{"details": "An unknown error occurred."}')
        await asyncio.sleep(1)

    @async_test
    async def test_multipage(self):
        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(
                {**mock_env(), 'FEEDS__1__SEED': (
                    'http://localhost:8081/'
                    'tests_fixture_activity_stream_multipage_1.json'
                )
                },
                mock_feed=read_file, mock_feed_status=lambda: 200,
                mock_headers=lambda: {},
            )
            results = await fetch_all_es_data_until(has_at_least(2))

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
            await self.setup_manual(env=env, mock_feed=read_file, mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            results = await fetch_all_es_data_until(has_at_least(4))

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
            await self.setup_manual(env=env, mock_feed=read_file, mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            results = await fetch_all_es_data_until(has_at_least(2))

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
            await self.setup_manual(env=env, mock_feed=read_file, mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            results_dict = await fetch_all_es_data_until(has_two_zendesk_tickets)

        results = json.dumps(results_dict)
        self.assertIn('"dit:zendesk:Ticket:1"', results)
        self.assertIn('"dit:zendesk:Ticket:1:Create"', results)
        self.assertIn('"2011-04-12T12:48:13+00:00"', results)
        self.assertIn('"dit:zendesk:Ticket:3"', results)
        self.assertIn('"dit:zendesk:Ticket:3:Create"', results)
        self.assertIn('"2011-04-12T12:48:13+00:00"', results)

    @async_test
    async def test_aventri(self):
        def aventri_fetch(results):
            if 'hits' not in results or 'hits' not in results['hits']:
                return False
            aventri_events = [
                item
                for item in results['hits']['hits']
                for source in [item['_source']]
                if 'dit:application' in source and source['dit:application'] == 'aventri'
            ]
            return len(aventri_events) == 4

        env = {
            **mock_env(),
            'FEEDS__1__UNIQUE_ID': 'third_feed',
            'FEEDS__1__ACCOUNT_ID': '1234',
            'FEEDS__1__API_KEY': '5678',
            'FEEDS__1__TYPE': 'aventri',
            'FEEDS__1__SEED':
            'http://localhost:8081/tests_fixture_aventri_listEvents_deleted.json',
            'FEEDS__1__AUTH_URL': 'http://localhost:8081/tests_fixture_aventri_authorize.json',
            'FEEDS__1__ATTENDEES_LIST_URL':
            'http://localhost:8081/tests_fixture_aventri_listAttendees.json',
            'FEEDS__1__EVENT_QUESTIONS_LIST_URL':
            'http://localhost:8081/tests_fixture_aventri_listQuestions.json',
        }

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=env, mock_feed=read_file, mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            results_dict = await fetch_all_es_data_until(aventri_fetch)

        # Sort events by ID here, so we're not hit by flakiness in case the results from
        # OpenSearch are not guarenteed to be sorted
        sources = [hit['_source'] for hit in results_dict['hits']['hits']]
        events = sorted(
            [source for source in sources if source['type'] == 'dit:aventri:Event'],
            key=lambda e: e['id']
        )
        attendees = sorted(
            [source for source in sources if source['type'] == 'dit:aventri:Attendee'],
            key=lambda a: a['id']
        )
        event = events[0]

        self.assertEqual(event['dit:application'], 'aventri')
        self.assertEqual(event['id'], 'dit:aventri:Event:1:Create')
        self.assertEqual(event['type'], 'dit:aventri:Event')

        self.assertEqual(
            event['object']['attributedTo'],
            {'type': 'dit:aventri:Folder', 'id': 'dit:aventri:Folder:Test'}
        )
        self.assertEqual(event['object']['id'], 'dit:aventri:Event:1')
        self.assertEqual(event['object']['type'], ['dit:aventri:Event'])
        self.assertEqual(event['object']['name'], 'Demo Event')
        self.assertEqual(event['object']['published'], '2018-08-23T04:37:39')
        self.assertEqual(event['object']['dit:aventri:closedate'], '2018-08-23T04:37:39')
        self.assertEqual(event['object']['startTime'], '2018-08-23T04:37:39')
        self.assertEqual(event['object']['endTime'], '2018-08-23T04:37:39')
        self.assertEqual(event['object']['dit:aventri:live_date'], '2018-08-23T00:00:00')
        self.assertEqual(event['object']['dit:aventri:location_city'], 'Melbourne')
        self.assertEqual(event['object']['dit:public'], True)

        attendee = attendees[0]

        self.assertEqual(attendee['dit:application'], 'aventri')
        self.assertEqual(attendee['id'], 'dit:aventri:Event:1:Attendee:1:Create')
        self.assertEqual(attendee['type'], 'dit:aventri:Attendee')

        self.assertEqual(
            attendee['object']['attributedTo'],
            {'type': 'dit:aventri:Event', 'id': 'dit:aventri:Event:1'}
        )
        self.assertEqual(attendee['object']['id'], 'dit:aventri:Attendee:1')
        self.assertEqual(attendee['object']['type'], ['dit:aventri:Attendee'])
        self.assertEqual(attendee['object']['published'], '2018-08-31T11:44:00')
        self.assertEqual(attendee['object']['dit:aventri:category'], 'Event Speaker')
        self.assertEqual(attendee['object']['dit:aventri:email'], 'test@test.com')
        self.assertEqual(attendee['object']['dit:aventri:firstname'], 'Steve')
        self.assertEqual(attendee['object']['dit:aventri:lastname'], 'Gates')
        self.assertEqual(attendee['object']['dit:aventri:companyname'], 'Applesoft')
        self.assertEqual(attendee['object']['dit:aventri:virtualeventattendance'], 'Yes')
        self.assertEqual(attendee['object']['dit:aventri:lastlobbylogin'], '2018-08-23T04:37:39')
        self.assertEqual(
            attendee['object']['dit:aventri:attendeeQuestions'],
            {'question_1': '1', 'question_2': 'Answer', 'question_3': '2'}
        )

        deleted_event = events[1]
        attendee_deleted_event = attendees[1]
        self.assertEqual(deleted_event['object']['type'], ['dit:aventri:Event', 'Tombstone'])
        self.assertEqual(attendee_deleted_event['object']['dit:aventri:attendeeQuestions'], None)

    @async_test
    async def test_aventri_attendee_with_no_company(self):
        def aventri_fetch(results):
            if 'hits' not in results or 'hits' not in results['hits']:
                return False
            aventri_events = [
                item
                for item in results['hits']['hits']
                for source in [item['_source']]
                if 'dit:application' in source and source['dit:application'] == 'aventri'
            ]
            return len(aventri_events) == 2

        env = {
            **mock_env(),
            'FEEDS__1__UNIQUE_ID': 'third_feed',
            'FEEDS__1__ACCOUNT_ID': '1234',
            'FEEDS__1__API_KEY': '5678',
            'FEEDS__1__TYPE': 'aventri',
            'FEEDS__1__SEED': 'http://localhost:8081/tests_fixture_aventri_listEvents.json',
            'FEEDS__1__AUTH_URL': 'http://localhost:8081/tests_fixture_aventri_authorize.json',
            'FEEDS__1__ATTENDEES_LIST_URL':
            'http://localhost:8081/tests_fixture_aventri_listAttendees_no_company.json',
            'FEEDS__1__EVENT_QUESTIONS_LIST_URL':
            'http://localhost:8081/tests_fixture_aventri_listQuestions_empty.json',
        }

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=env, mock_feed=read_file, mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            results_dict = await fetch_all_es_data_until(aventri_fetch)

        event = results_dict['hits']['hits'][0]['_source']

        self.assertEqual(event['dit:application'], 'aventri')
        self.assertEqual(event['id'], 'dit:aventri:Event:1:Create')
        self.assertEqual(event['type'], 'dit:aventri:Event')

        self.assertEqual(
            event['object']['attributedTo'],
            {'type': 'dit:aventri:Folder', 'id': 'dit:aventri:Folder:Test'}
        )
        self.assertEqual(event['object']['id'], 'dit:aventri:Event:1')
        self.assertEqual(event['object']['type'], ['dit:aventri:Event'])
        self.assertEqual(event['object']['name'], 'Demo Event')
        self.assertEqual(event['object']['published'], '2018-08-23T04:37:39')
        self.assertEqual(event['object']['dit:aventri:closedate'], '2018-08-23T04:37:39')
        self.assertEqual(event['object']['startTime'], '2018-08-23T04:37:39')
        self.assertEqual(event['object']['endTime'], '2018-08-23T04:37:39')
        self.assertEqual(event['object']['dit:aventri:live_date'], '2018-08-23T00:00:00')
        self.assertEqual(event['object']['dit:aventri:location_city'], 'Melbourne')
        self.assertEqual(event['object']['dit:public'], True)

        attendee = results_dict['hits']['hits'][1]['_source']

        self.assertEqual(attendee['dit:application'], 'aventri')
        self.assertEqual(attendee['id'], 'dit:aventri:Event:1:Attendee:1:Create')
        self.assertEqual(attendee['type'], 'dit:aventri:Attendee')

        self.assertEqual(
            attendee['object']['attributedTo'],
            {'type': 'dit:aventri:Event', 'id': 'dit:aventri:Event:1'}
        )
        self.assertEqual(attendee['object']['id'], 'dit:aventri:Attendee:1')
        self.assertEqual(attendee['object']['type'], ['dit:aventri:Attendee'])
        self.assertEqual(attendee['object']['published'], '2018-08-31T11:44:00')
        self.assertEqual(attendee['object']['dit:aventri:category'], 'Event Speaker')
        self.assertEqual(attendee['object']['dit:aventri:email'], 'test@test.com')
        self.assertEqual(attendee['object']['dit:aventri:firstname'], 'Steve')
        self.assertEqual(attendee['object']['dit:aventri:lastname'], 'Gates')
        self.assertEqual(attendee['object']['dit:aventri:companyname'], None)
        self.assertEqual(attendee['object']['dit:aventri:virtualeventattendance'], 'Yes')
        self.assertEqual(attendee['object']['dit:aventri:lastlobbylogin'], None)
        self.assertEqual(attendee['object']['dit:aventri:attendeeQuestions'], {})

    @async_test
    async def test_aventri_event_with_zero_dates(self):
        def aventri_fetch(results):
            if 'hits' not in results or 'hits' not in results['hits']:
                return False
            aventri_events = [
                item
                for item in results['hits']['hits']
                for source in [item['_source']]
                if 'dit:application' in source and source['dit:application'] == 'aventri'
            ]
            return len(aventri_events) == 1

        env = {
            **mock_env(),
            'FEEDS__1__UNIQUE_ID': 'third_feed',
            'FEEDS__1__ACCOUNT_ID': '1234',
            'FEEDS__1__API_KEY': '5678',
            'FEEDS__1__TYPE': 'aventri',
            'FEEDS__1__SEED':
            'http://localhost:8081/tests_fixture_aventri_listEvents_zero_dates.json',
            'FEEDS__1__AUTH_URL': 'http://localhost:8081/tests_fixture_aventri_authorize.json',
            'FEEDS__1__ATTENDEES_LIST_URL':
            'http://localhost:8081/tests_fixture_aventri_listAttendees_empty.json',
            'FEEDS__1__EVENT_QUESTIONS_LIST_URL':
            'http://localhost:8081/tests_fixture_aventri_listQuestions.json',
        }

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=env, mock_feed=read_file, mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            results_dict = await fetch_all_es_data_until(aventri_fetch)

        event = results_dict['hits']['hits'][0]['_source']

        self.assertEqual(event['dit:application'], 'aventri')
        self.assertEqual(event['id'], 'dit:aventri:Event:1:Create')
        self.assertEqual(event['type'], 'dit:aventri:Event')

        self.assertEqual(
            event['object']['attributedTo'],
            {'type': 'dit:aventri:Folder', 'id': 'dit:aventri:Folder:Test'}
        )
        self.assertEqual(event['object']['id'], 'dit:aventri:Event:1')
        self.assertEqual(event['object']['type'], ['dit:aventri:Event'])
        self.assertEqual(event['object']['name'], 'Demo Event')
        self.assertEqual(event['object']['published'], '2018-08-23T04:37:39')
        self.assertEqual(event['object']['dit:aventri:location_city'], 'Melbourne')
        self.assertEqual(event['object']['dit:public'], True)
        self.assertEqual(event['object']['dit:aventri:closedate'], None)
        self.assertEqual(event['object']['startTime'], None)
        self.assertEqual(event['object']['endTime'], None)

    @async_test
    async def test_aventri_event_with_null_dates(self):
        def aventri_fetch(results):
            if 'hits' not in results or 'hits' not in results['hits']:
                return False
            aventri_events = [
                item
                for item in results['hits']['hits']
                for source in [item['_source']]
                if 'dit:application' in source and source['dit:application'] == 'aventri'
            ]
            return len(aventri_events) == 1

        env = {
            **mock_env(),
            'FEEDS__1__UNIQUE_ID': 'third_feed',
            'FEEDS__1__ACCOUNT_ID': '1234',
            'FEEDS__1__API_KEY': '5678',
            'FEEDS__1__TYPE': 'aventri',
            'FEEDS__1__SEED':
            'http://localhost:8081/tests_fixture_aventri_listEvents_null_dates.json',
            'FEEDS__1__AUTH_URL': 'http://localhost:8081/tests_fixture_aventri_authorize.json',
            'FEEDS__1__ATTENDEES_LIST_URL':
            'http://localhost:8081/tests_fixture_aventri_listAttendees_empty.json',
            'FEEDS__1__EVENT_QUESTIONS_LIST_URL':
            'http://localhost:8081/tests_fixture_aventri_listQuestions.json',
        }

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=env, mock_feed=read_file, mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            results_dict = await fetch_all_es_data_until(aventri_fetch)

        event = results_dict['hits']['hits'][0]['_source']

        self.assertEqual(event['dit:application'], 'aventri')
        self.assertEqual(event['id'], 'dit:aventri:Event:1:Create')
        self.assertEqual(event['type'], 'dit:aventri:Event')

        self.assertEqual(
            event['object']['attributedTo'],
            {'type': 'dit:aventri:Folder', 'id': 'dit:aventri:Folder:Test'}
        )
        self.assertEqual(event['object']['id'], 'dit:aventri:Event:1')
        self.assertEqual(event['object']['type'], ['dit:aventri:Event'])
        self.assertEqual(event['object']['name'], 'Demo Event')
        self.assertEqual(event['object']['published'], '2018-08-23T04:37:39')
        self.assertEqual(event['object']['dit:aventri:location_city'], 'Melbourne')
        self.assertEqual(event['object']['dit:public'], True)
        self.assertEqual(event['object']['dit:aventri:closedate'], None)
        self.assertEqual(event['object']['startTime'], None)
        self.assertEqual(event['object']['endTime'], None)

    @async_test
    async def test_maxemail(self):
        def maxemail_base_fetch(results):
            if 'hits' not in results or 'hits' \
                    not in results['hits'] or len(results['hits']['hits']) < 16:
                return False

            if str(results).find('maxemail') != -1:
                return True

            return False

        env = {
            **mock_env(),
            'FEEDS__1__UNIQUE_ID': 'maxemail',
            'FEEDS__1__USERNAME': 'foo',
            'FEEDS__1__PASSWORD': 'bar',
            'FEEDS__1__PAGE_SIZE': '8',
            'FEEDS__1__SEED': 'http://localhost:6098/api/json/data_export_quick',
            'FEEDS__1__DATA_EXPORT_URL': 'http://localhost:8081/tests_fixture_maxemail_{key}.csv',
            'FEEDS__1__CAMPAIGN_URL': 'http://localhost:8081/tests_fixture_maxemail_campaign.json',
            'FEEDS__1__TYPE': 'maxemail',
        }

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=env, mock_feed=read_file, mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})

        # Setup Mock data export Server
        async def mock_get_download_key(request):
            method = (await request.post())['method']
            return web.Response(text=f'"{method}"')

        await _web_application(
            port=6098,
            routes=[web.post('/api/json/data_export_quick', mock_get_download_key)],
        )

        results_dict = await fetch_all_es_data_until(maxemail_base_fetch)

        self.assertEqual(len(results_dict['hits']['hits']), 16)
        ids = [item['_source']['object']['id'] for item in results_dict['hits']['hits']]
        self.assertTrue('dit:maxemail:Email:Sent:459:2020-09-11T16:13:03:a.b@dummy.co' in ids)
        self.assertTrue('dit:maxemail:Email:Clicked:459:2020-11-03T01:45:08:a.b@dummy.co' in ids)
        self.assertTrue('dit:maxemail:Email:Opened:459:2020-11-03T00:06:54:a.b@dummy.co' in ids)
        self.assertTrue('dit:maxemail:Email:Responded:459:2020-11-03T01:45:08:a.b@dummy.co' in ids)
        self.assertTrue(
            'dit:maxemail:Email:Unsubscribed:459:2020-11-03T08:38:19:a.b@dummy.co' in ids)
        self.assertTrue('dit:maxemail:Campaign:459' in ids)

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
            await self.setup_manual(env=mock_env(), mock_feed=read_file_broken_then_fixed,
                                    mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            results = await fetch_all_es_data_until(has_at_least(1))

        self.assertIn(
            'dit:exportOpportunities:Enquiry:49863:Create',
            str(results),
        )

    @async_test
    async def test_on_feed_401_retries(self):
        sent_401 = False

        def send_401_then_200():
            nonlocal sent_401
            status = 401 if sent_401 else 200
            sent_401 = True
            return status

        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_file,
                                    mock_feed_status=send_401_then_200,
                                    mock_headers=lambda: {})
            results = await fetch_all_es_data_until(has_at_least(1))

        self.assertIn(
            'dit:exportOpportunities:Enquiry:49863:Create',
            str(results),
        )

    @async_test
    async def test_on_feed_429_retries(self):
        sent_429 = False
        sent_retry_after = False

        def send_429_then_200():
            nonlocal sent_429
            status = 200 if sent_429 else 429
            sent_429 = True
            return status

        def send_retry_after_then_blank():
            nonlocal sent_retry_after
            headers = {} if sent_retry_after else {
                'Retry-After': '7',
            }
            sent_retry_after = True
            return headers

        with patch('asyncio.sleep', wraps=fast_sleep) as mock_sleep:
            await self.setup_manual(env=mock_env(), mock_feed=read_file,
                                    mock_feed_status=send_429_then_200,
                                    mock_headers=send_retry_after_then_blank)
            results = await fetch_all_es_data_until(has_at_least(1))
            mock_sleep.assert_any_call(7)

        self.assertIn(
            'dit:exportOpportunities:Enquiry:49863:Create',
            str(results),
        )

    @async_test
    async def test_returns_some_metrics(self):
        with patch('asyncio.sleep', wraps=fast_sleep):
            await self.setup_manual(env=mock_env(), mock_feed=read_file,
                                    mock_feed_status=lambda: 200, mock_headers=lambda: {})
            await fetch_all_es_data_until(has_at_least(2))

        async with aiohttp.ClientSession() as session:
            for _ in range(0, 90):
                url = 'http://127.0.0.1:8080/metrics'
                result = await session.get(url)
                text = await result.text()
                is_success = 'status="success"' in text and \
                    '{feed_unique_id="first_feed",searchable="searchable"} 2.0' in text
                sleep_time = 0 if is_success else 1
                await ORIGINAL_SLEEP(sleep_time)

        self.assertIn('python_info', text)
        self.assertIn('ingest_feed_duration_seconds_count', text)
        self.assertIn('feed_unique_id="first_feed"', text)
        self.assertIn('status="success"', text)
        self.assertIn('ingest_activities_nonunique_total{', text)

        # The order of labels is apparently not deterministic
        self.assertIn('ingest_page_duration_seconds_bucket{', text)
        self.assertIn('ingest_type="full"', text)
        self.assertIn('le="0.005"', text)
        self.assertIn('stage="push"', text)

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
            await self.setup_manual(env=env, mock_feed=read_file, mock_feed_status=lambda: 200,
                                    mock_headers=lambda: {})
            await wait_until_get_working()
            await ORIGINAL_SLEEP(2)

            async with aiohttp.ClientSession() as session:
                url = 'http://127.0.0.1:8080/metrics'
                result = await session.get(url)

        self.assertIn('status="success"', await result.text())

    @async_test
    async def test_if_lost_lock_then_raise(self):
        async def mock_close():
            await ORIGINAL_SLEEP(0)

        with patch('asyncio.sleep', wraps=fast_sleep), patch('raven.Client') as raven_client:
            raven_client().remote.get_transport().close = mock_close
            await self.setup_manual(env=mock_env(), mock_feed=read_file,
                                    mock_feed_status=lambda: 200, mock_headers=lambda: {})
            redis_client = await aioredis.create_redis('redis://127.0.0.1:6379')
            await redis_client.execute('DEL', 'lock')
            await ORIGINAL_SLEEP(2)

        raven_client().captureMessage.assert_called()
