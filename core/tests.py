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
import mohawk

from core.app import run_application
from core.app_utils import flatten


class TestBase(unittest.TestCase):

    def setup_manual(self, env, mock_feed):
        ''' Test setUp function that can be customised on a per-test basis '''

        self.addCleanup(self.teardown_manual)

        self.os_environ_patcher = patch.dict(os.environ, env)
        self.os_environ_patcher.start()
        self.loop = asyncio.get_event_loop()

        self.feed_requested = [asyncio.Future(), asyncio.Future()]

        def feed_requested_callback(request):
            try:
                first_not_done = next(
                    future for future in self.feed_requested if not future.done())
            except StopIteration:
                pass
            else:
                first_not_done.set_result(request)

        self.feed_runner_1 = \
            self.loop.run_until_complete(
                run_feed_application(mock_feed, feed_requested_callback, 8081),
            )

        original_app_runner = aiohttp.web.AppRunner

        def wrapped_app_runner(*args, **kwargs):
            self.app_runner = original_app_runner(*args, **kwargs)
            return self.app_runner

        self.app_runner_patcher = patch('aiohttp.web.AppRunner', wraps=wrapped_app_runner)
        self.app_runner_patcher.start()
        self.loop.run_until_complete(delete_all_es_data())

    def teardown_manual(self):
        for task in asyncio.Task.all_tasks():
            task.cancel()
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(asyncio.gather(*flatten([
            ([self.app_runner.cleanup()] if hasattr(self, 'app_runner') else []) +
            ([self.feed_runner_1.cleanup()] if hasattr(self, 'feed_runner_1') else [])
        ])))
        if hasattr(self, 'app_runner_patcher'):
            self.app_runner_patcher.stop()
        if hasattr(self, 'os_environ_patcher'):
            self.os_environ_patcher.stop()


class TestConnection(TestBase):

    def test_application_accepts_http(self):
        self.setup_manual(
            env=mock_env(),
            mock_feed=read_file,
        )

        asyncio.ensure_future(run_application())
        self.assertTrue(is_http_accepted_eventually())


class TestAuthentication(TestBase):

    def test_no_auth_then_401(self):
        self.setup_manual(env=mock_env(), mock_feed=read_file)

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        text, status = self.loop.run_until_complete(post_no_auth(url, '1.2.3.4'))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Authentication credentials were not provided."}')

    def test_bad_id_then_401(self):
        self.setup_manual(env=mock_env(), mock_feed=read_file)

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-incorrect', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(post(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_bad_secret_then_401(self):
        self.setup_manual(env=mock_env(), mock_feed=read_file)

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-2', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(post(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_bad_method_then_401(self):
        self.setup_manual(env=mock_env(), mock_feed=read_file)

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '', 'application/json',
        )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(post(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_bad_content_then_401(self):
        self.setup_manual(env=mock_env(), mock_feed=read_file)

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', 'content', '',
        )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(post(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_bad_content_type_then_401(self):
        self.setup_manual(env=mock_env(), mock_feed=read_file)

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', 'some-type',
        )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(post(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_no_content_type_then_401(self):
        self.setup_manual(env=mock_env(), mock_feed=read_file)

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', 'some-type',
        )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(
            post_no_content_type(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertIn('Content-Type header was not set.', text)

    def test_time_skew_then_401(self):
        self.setup_manual(env=mock_env(), mock_feed=read_file)

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        past = datetime.datetime.now() + datetime.timedelta(seconds=-61)
        with freeze_time(past):
            auth = hawk_auth_header(
                'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
            )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(post(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_repeat_auth_then_401(self):
        self.setup_manual(env=mock_env(), mock_feed=read_file)

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4'
        _, status_1 = self.loop.run_until_complete(post(url, auth, x_forwarded_for))
        self.assertEqual(status_1, 200)

        text_2, status_2 = self.loop.run_until_complete(post(url, auth, x_forwarded_for))
        self.assertEqual(status_2, 401)
        self.assertEqual(text_2, '{"details": "Incorrect authentication credentials."}')

    def test_nonces_cleared(self):
        ''' Makes duplicate requests, but with the code patched so the nonce expiry time
            is shorter then the allowed Hawk skew. The second request succeeding gives
            evidence that the cache of nonces was cleared.
        '''
        self.setup_manual(env=mock_env(), mock_feed=read_file)

        now = datetime.datetime.now()
        past = now + datetime.timedelta(seconds=-45)

        with patch('core.app.NONCE_EXPIRE', 30):
            asyncio.ensure_future(run_application())
            is_http_accepted_eventually()

            url = 'http://127.0.0.1:8080/v1/'
            x_forwarded_for = '1.2.3.4'

            with freeze_time(past):
                auth = hawk_auth_header(
                    'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
                )
                _, status_1 = self.loop.run_until_complete(
                    post(url, auth, x_forwarded_for))
            self.assertEqual(status_1, 200)

            with freeze_time(now):
                _, status_2 = self.loop.run_until_complete(
                    post(url, auth, x_forwarded_for))
            self.assertEqual(status_2, 200)

    def test_no_x_forwarded_for_401(self):
        self.setup_manual(env=mock_env(), mock_feed=read_file)

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        text, status = self.loop.run_until_complete(post_no_x_forwarded_for(url, auth))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_bad_x_forwarded_for_401(self):
        self.setup_manual(env=mock_env(), mock_feed=read_file)

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        x_forwarded_for = '3.4.5.6'
        text, status = self.loop.run_until_complete(post(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_at_end_x_forwarded_for_401(self):
        self.setup_manual(env=mock_env(), mock_feed=read_file)

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        x_forwarded_for = '3.4.5.6,1.2.3.4'
        text, status = self.loop.run_until_complete(post(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_second_id_returns_object(self):
        self.setup_manual(env=mock_env(), mock_feed=read_file)

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-2', 'incoming-some-secret-2', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(post(url, auth, x_forwarded_for))
        self.assertEqual(status, 200)
        self.assertEqual(text, '{"secret": "to-be-hidden"}')

    def test_post_returns_object(self):
        self.setup_manual(env=mock_env(), mock_feed=read_file)

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(post(url, auth, x_forwarded_for))
        self.assertEqual(status, 200)
        self.assertEqual(text, '{"secret": "to-be-hidden"}')

    def test_post_creds_get_403(self):
        self.setup_manual(env=mock_env(), mock_feed=read_file)

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '', 'application/json',
        )
        x_forwarded_for = '1.2.3.4'
        text, status, _ = self.loop.run_until_complete(get(url, auth, x_forwarded_for, b''))
        self.assertEqual(status, 403)
        self.assertEqual(text, '{"details": "You are not authorized to perform this action."}')


class TestApplication(TestBase):

    def test_get_returns_feed_data(self):
        def has_at_least_two_results(results):
            return (
                'hits' in results and
                'hits' in results['hits'] and
                len(results['hits']['hits']) >= 2
            )

        self.setup_manual(env=mock_env(), mock_feed=read_file)

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        x_forwarded_for = '1.2.3.4'

        result, status, headers = self.loop.run_until_complete(
            get_until(url, x_forwarded_for, has_at_least_two_results, asyncio.sleep))
        self.assertEqual(status, 200)
        self.assertEqual(result['hits']['hits'][0]['_id'],
                         'dit:exportOpportunities:Enquiry:49863:Create')
        self.assertEqual(result['hits']['hits'][1]['_id'],
                         'dit:exportOpportunities:Enquiry:49862:Create')
        self.assertEqual(headers['Server'], 'activity-stream')
        self.assertEqual(headers['Server'], 'activity-stream')

    def test_get_can_filter(self):
        def has_at_least_four_results(results):
            return (
                'hits' in results and
                'hits' in results['hits'] and
                len(results['hits']['hits']) >= 4
            )

        env = {
            **mock_env(),
            'FEEDS__2__SEED': 'http://localhost:8081/tests_fixture_zendesk_1.json',
            'FEEDS__2__API_EMAIL': 'test@test.com',
            'FEEDS__2__API_KEY': 'some-key',
            'FEEDS__2__TYPE': 'zendesk',
        }
        self.setup_manual(env=env, mock_feed=read_file)

        original_sleep = asyncio.sleep

        async def fast_sleep(_):
            await original_sleep(0)

        async def _test():
            with patch('asyncio.sleep', wraps=fast_sleep):
                asyncio.ensure_future(run_application())
                return await fetch_all_es_data_until(has_at_least_four_results, original_sleep)

        self.loop.run_until_complete(_test())

        query = json.dumps({
            'query': {
                'range': {
                    'published': {
                        'gte': '2011-04-12',
                        'lte': '2011-04-12',
                    },
                },
            },
        }).encode('utf-8')

        url = 'http://127.0.0.1:8080/v1/'
        x_forwarded_for = '1.2.3.4'
        auth = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url, 'GET', query, 'application/json',
        )
        result, status, _ = self.loop.run_until_complete(
            get(url, auth, x_forwarded_for, query))
        self.assertEqual(status, 200)
        data = json.loads(result)
        self.assertEqual(data['hits']['total'], 1)
        self.assertEqual(data['hits']['hits'][0]['_id'],
                         'dit:zendesk:Ticket:3:Create')
        self.assertEqual(data['hits']['hits'][0]['_source']['published'],
                         '2011-04-12T12:48:13+00:00')

    @freeze_time('2012-01-14 12:00:01')
    @patch('os.urandom', return_value=b'something-random')
    def test_single_page(self, _):
        posted_to_es_once, append_es = append_until(lambda results: len(results) == 1)

        self.setup_manual(env={**mock_env(), 'ELASTICSEARCH__PORT': '9201'},
                          mock_feed=read_file)

        async def return_200_and_callback(request):
            content, headers = (await request.content.read(), request.headers)
            asyncio.get_event_loop().call_soon(append_es, (content, headers))
            return return_200(request)

        routes = [
            web.post('/_bulk', return_200_and_callback),
        ]
        es_runner = self.loop.run_until_complete(
            run_es_application(port=9201, override_routes=routes))
        asyncio.ensure_future(run_application())

        async def _test():
            return await posted_to_es_once

        [[es_bulk_content, es_bulk_headers]] = self.loop.run_until_complete(_test())
        es_bulk_request_dicts = [
            json.loads(line)
            for line in es_bulk_content.split(b'\n')[0:-1]
        ]

        self.loop.run_until_complete(es_runner.cleanup())

        self.assertEqual(self.feed_requested[0].result(
        ).headers['Authorization'], (
            'Hawk '
            'mac="lTF8bPQSP4HU6oQcassERsIl8DNDbiu6jXhbcdUTKIg=", '
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
            'Signature=ce881b6b5506fb0b6a8c1ab8f79ef7d162020a98f90fc2a5abfc62c3eb23ffb8')
        self.assertEqual(es_bulk_content.decode('utf-8')[-1], '\n')
        self.assertEqual(es_bulk_headers['Content-Type'], 'application/x-ndjson')

        self.assertEqual(es_bulk_request_dicts[0]['index']['_index'], 'activities')
        self.assertEqual(es_bulk_request_dicts[0]['index']['_type'], '_doc')
        self.assertEqual(es_bulk_request_dicts[0]['index']
                         ['_id'], 'dit:exportOpportunities:Enquiry:49863:Create')
        self.assertEqual(es_bulk_request_dicts[1]['published'], '2018-04-12T12:48:13+00:00')
        self.assertEqual(es_bulk_request_dicts[1]['type'], 'Create')
        self.assertEqual(es_bulk_request_dicts[1]['object']
                         ['type'][1], 'dit:exportOpportunities:Enquiry')
        self.assertEqual(es_bulk_request_dicts[1]['actor']['dit:companiesHouseNumber'], '123432')

        self.assertEqual(es_bulk_request_dicts[2]['index']['_index'], 'activities')
        self.assertEqual(es_bulk_request_dicts[2]['index']['_type'], '_doc')
        self.assertEqual(es_bulk_request_dicts[2]['index']
                         ['_id'], 'dit:exportOpportunities:Enquiry:49862:Create')
        self.assertEqual(es_bulk_request_dicts[3]['published'], '2018-03-23T17:06:53+00:00')
        self.assertEqual(es_bulk_request_dicts[3]['type'], 'Create')
        self.assertEqual(es_bulk_request_dicts[3]['object']
                         ['type'][1], 'dit:exportOpportunities:Enquiry')
        self.assertEqual(es_bulk_request_dicts[3]['actor']['dit:companiesHouseNumber'], '82312')

    def test_es_401_is_500(self):
        self.setup_manual(env={**mock_env(), 'ELASTICSEARCH__PORT': '9201'},
                          mock_feed=read_file)
        routes = [
            web.post('/_search', return_401),
        ]
        es_runner = self.loop.run_until_complete(
            run_es_application(port=9201, override_routes=routes))
        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url, 'GET', '', 'application/json',
        )
        x_forwarded_for = '1.2.3.4'
        text, status, _ = self.loop.run_until_complete(get(url, auth, x_forwarded_for, b''))
        self.loop.run_until_complete(es_runner.cleanup())

        self.assertEqual(status, 500)
        self.assertEqual(text, '{"details": "An unknown error occurred."}')

    def test_es_no_connect_on_get_500(self):
        self.setup_manual({
            **mock_env(),
            'ELASTICSEARCH__PORT': '9201'
        }, mock_feed=read_file)
        es_runner = self.loop.run_until_complete(
            run_es_application(port=9201, override_routes=[]))

        asyncio.ensure_future(run_application())
        is_http_accepted_eventually()

        self.loop.run_until_complete(es_runner.cleanup())
        url = 'http://127.0.0.1:8080/v1/'
        auth = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url, 'GET', '', 'application/json',
        )
        x_forwarded_for = '1.2.3.4'
        text, status, _ = self.loop.run_until_complete(get(url, auth, x_forwarded_for, b''))

        self.assertEqual(status, 500)
        self.assertEqual(text, '{"details": "An unknown error occurred."}')

    def test_multipage(self):
        def has_at_least_two_results(results):
            return (
                'hits' in results and
                'hits' in results['hits'] and
                len(results['hits']['hits']) >= 2
            )

        self.setup_manual(
            {**mock_env(), 'FEEDS__1__SEED': (
                'http://localhost:8081/'
                'tests_fixture_elasticsearch_bulk_multipage_1.json'
            )
            },
            mock_feed=read_file,
        )

        original_sleep = asyncio.sleep

        async def fast_sleep(_):
            await original_sleep(0)

        async def _test():
            with patch('asyncio.sleep', wraps=fast_sleep) as mock_sleep:
                asyncio.ensure_future(run_application())
                mock_sleep.assert_not_called()
                result = await fetch_all_es_data_until(has_at_least_two_results, original_sleep)
                mock_sleep.assert_any_call(0)
                return result

        results = self.loop.run_until_complete(_test())
        self.assertIn('dit:exportOpportunities:Enquiry:4986999:Create',
                      str(results))

    def test_two_feeds(self):
        def has_at_least_four_results(results):
            return (
                'hits' in results and
                'hits' in results['hits'] and
                len(results['hits']['hits']) >= 4
            )

        env = {
            **mock_env(),
            'FEEDS__2__SEED': 'http://localhost:8081/tests_fixture_elasticsearch_bulk_2.json',
            'FEEDS__2__ACCESS_KEY_ID': 'feed-some-id',
            'FEEDS__2__SECRET_ACCESS_KEY': '?[!@$%^%',
            'FEEDS__2__TYPE': 'elasticsearch_bulk',
        }
        self.setup_manual(env=env, mock_feed=read_file)

        original_sleep = asyncio.sleep

        async def fast_sleep(_):
            await original_sleep(0)

        async def _test():
            with patch('asyncio.sleep', wraps=fast_sleep):
                asyncio.ensure_future(run_application())
                return await fetch_all_es_data_until(has_at_least_four_results, original_sleep)

        results = self.loop.run_until_complete(_test())
        self.assertIn('dit:exportOpportunities:Enquiry:49863:Create', str(results))
        self.assertIn('dit:exportOpportunities:Enquiry:42863:Create', str(results))

    def test_zendesk(self):
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
            'FEEDS__2__SEED': 'http://localhost:8081/tests_fixture_zendesk_1.json',
            'FEEDS__2__API_EMAIL': 'test@test.com',
            'FEEDS__2__API_KEY': 'some-key',
            'FEEDS__2__TYPE': 'zendesk',
        }
        self.setup_manual(env=env, mock_feed=read_file)

        original_sleep = asyncio.sleep

        async def fast_sleep(_):
            await original_sleep(0)

        async def _test():
            with patch('asyncio.sleep', wraps=fast_sleep):
                asyncio.ensure_future(run_application())
                return await fetch_all_es_data_until(has_two_zendesk_tickets, original_sleep)

        results = json.dumps(self.loop.run_until_complete(_test()))
        self.assertIn('"dit:zendesk:Ticket:1"', results)
        self.assertIn('"dit:zendesk:Ticket:1:Create"', results)
        self.assertIn('"2011-04-12T12:48:13+00:00"', results)
        self.assertIn('"dit:zendesk:Ticket:3"', results)
        self.assertIn('"dit:zendesk:Ticket:3:Create"', results)
        self.assertIn('"2011-04-12T12:48:13+00:00"', results)

    def test_on_bad_json_retries(self):
        def has_at_least_one_result(results):
            return (
                'hits' in results and
                'hits' in results['hits'] and
                len(results['hits']['hits']) >= 1
            )

        sent_broken = False

        def read_file_broken_then_fixed(path):
            nonlocal sent_broken

            feed_contents_maybe_broken = (
                read_file(path) +
                ('something-invalid' if not sent_broken else '')
            )
            sent_broken = True
            return feed_contents_maybe_broken

        self.setup_manual(env=mock_env(), mock_feed=read_file_broken_then_fixed)

        original_sleep = asyncio.sleep

        async def fast_sleep(_):
            await original_sleep(0)

        async def _test():
            with patch('asyncio.sleep', wraps=fast_sleep) as mock_sleep:
                asyncio.ensure_future(run_application())
                mock_sleep.assert_not_called()
                results = await fetch_all_es_data_until(has_at_least_one_result, original_sleep)
                mock_sleep.assert_any_call(60)
                return results

        es_results = self.loop.run_until_complete(_test())

        self.assertIn(
            'dit:exportOpportunities:Enquiry:49863:Create',
            str(es_results),
        )


class TestProcess(unittest.TestCase):

    def setUp(self):
        loop = asyncio.get_event_loop()

        loop.run_until_complete(delete_all_es_data())
        self.feed_runner_1 = loop.run_until_complete(run_feed_application(read_file, Mock(), 8081))
        self.server = Popen([sys.executable, '-m', 'core.app'], env={
            **mock_env(),
            'COVERAGE_PROCESS_START': os.environ['COVERAGE_PROCESS_START'],
        })

    def tearDown(self):
        for task in asyncio.Task.all_tasks():
            task.cancel()
        self.server.terminate()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.feed_runner_1.cleanup())

    def test_server_accepts_http(self):
        self.assertTrue(is_http_accepted_eventually())


def is_http_accepted_eventually():
    loop = asyncio.get_event_loop()
    connected_future = asyncio.ensure_future(_is_http_accepted_eventually())
    return loop.run_until_complete(connected_future)


async def _is_http_accepted_eventually():
    def is_connection_error(exception):
        return 'Cannot connect to host' in str(exception)

    attempts = 0
    while attempts < 20:
        try:
            async with aiohttp.ClientSession() as session:
                await session.get('http://127.0.0.1:8080', timeout=1)
            return True
        except aiohttp.client_exceptions.ClientConnectorError as exception:
            attempts += 1
            await asyncio.sleep(0.2)
            if not is_connection_error(exception):
                return True

    return False


def read_file(path):
    with open('core/' + path, 'rb') as file:
        return file.read().decode('utf-8')


async def run_feed_application(feed, feed_requested_callback, port):
    async def handle(request):
        path = request.match_info['feed']
        asyncio.get_event_loop().call_soon(feed_requested_callback, request)
        return web.Response(text=feed(path))

    app = web.Application()
    app.add_routes([web.get('/{feed}', handle)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', port)
    await site.start()
    return runner


async def delete_all_es_data():
    async with aiohttp.ClientSession() as session:
        await session.delete('http://127.0.0.1:9200/*')

    def has_no_results(results):
        return (
            'hits' in results and
            'hits' in results['hits'] and
            len(results['hits']['hits']) >= 0
        )

    await fetch_all_es_data_until(has_no_results, asyncio.sleep)


async def fetch_all_es_data_until(condition, sleep):

    async def fetch_all_es_data():
        async with aiohttp.ClientSession() as session:
            results = await session.get('http://127.0.0.1:9200/_search')
            return json.loads(await results.text())

    while True:
        all_es_data = await fetch_all_es_data()
        if condition(all_es_data):
            break
        await sleep(0.05)

    return all_es_data


def append_until(condition):
    future = asyncio.Future()

    all_data = []

    def append(data):
        if not future.done():
            all_data.append(data)
        if condition(all_data):
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
        }, data=body, timeout=1)
    return (await result.text(), result.status, result.headers)


async def get_until(url, x_forwarded_for, condition, sleep):
    while True:
        auth = hawk_auth_header(
            'incoming-some-id-3', 'incoming-some-secret-3', url, 'GET', '', 'application/json',
        )
        all_data, status, headers = await get(url, auth, x_forwarded_for, b'')
        dict_data = json.loads(all_data)
        if condition(dict_data):
            break
        await sleep(0.05)

    return dict_data, status, headers


async def post(url, auth, x_forwarded_for):
    async with aiohttp.ClientSession() as session:
        result = await session.post(url, headers={
            'Authorization': auth,
            'Content-Type': '',
            'X-Forwarded-For': x_forwarded_for,
        }, timeout=1)
    return (await result.text(), result.status)


async def post_no_auth(url, x_forwarded_for):
    async with aiohttp.ClientSession() as session:
        result = await session.post(url, headers={
            'Content-Type': '',
            'X-Forwarded-For': x_forwarded_for,
        }, timeout=1)
    return (await result.text(), result.status)


async def post_no_x_forwarded_for(url, auth):
    async with aiohttp.ClientSession() as session:
        headers = {
            'Authorization': auth,
            'Content-Type': '',
        }
        result = await session.post(url, headers=headers, timeout=1)
    return (await result.text(), result.status)


async def post_no_content_type(url, auth, x_forwarded_for):
    async with aiohttp.ClientSession(skip_auto_headers=['Content-Type']) as session:
        result = await session.post(url, headers={
            'Authorization': auth,
            'X-Forwarded-For': x_forwarded_for,
        }, timeout=1)

    return (await result.text(), result.status)


async def return_200(_):
    return web.Response(text='{}', status=200, content_type='application/json')


async def return_401(_):
    return web.Response(text='{}', status=401, content_type='application/json')


async def run_es_application(port, override_routes):
    default_routes = [
        web.put('/activities/_mapping/_doc', return_200),
        web.put('/activities', return_200),
        web.get('/_search', return_200),
        web.post('/_bulk', return_200),
    ]

    routes_no_duplicates = {
        (route.method, route.path): route
        for route in (default_routes+override_routes)
    }.values()

    return await _web_application(port=port, routes=routes_no_duplicates)


async def _web_application(port, routes):
    app = web.Application()
    app.add_routes(routes)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', port)
    await site.start()
    return runner


def mock_env():
    return {
        'PORT': '8080',
        'ELASTICSEARCH__AWS_ACCESS_KEY_ID': 'some-id',
        'ELASTICSEARCH__AWS_SECRET_ACCESS_KEY': 'aws-secret',
        'ELASTICSEARCH__HOST': '127.0.0.1',
        'ELASTICSEARCH__PORT': '9200',
        'ELASTICSEARCH__PROTOCOL': 'http',
        'ELASTICSEARCH__REGION': 'us-east-2',
        'FEEDS__1__SEED': 'http://localhost:8081/tests_fixture_elasticsearch_bulk_1.json',
        'FEEDS__1__ACCESS_KEY_ID': 'feed-some-id',
        'FEEDS__1__SECRET_ACCESS_KEY': '?[!@$%^%',
        'FEEDS__1__TYPE': 'elasticsearch_bulk',
        'INCOMING_ACCESS_KEY_PAIRS__1__KEY_ID': 'incoming-some-id-1',
        'INCOMING_ACCESS_KEY_PAIRS__1__SECRET_KEY': 'incoming-some-secret-1',
        'INCOMING_ACCESS_KEY_PAIRS__1__PERMISSIONS__1': 'POST',
        'INCOMING_ACCESS_KEY_PAIRS__2__KEY_ID': 'incoming-some-id-2',
        'INCOMING_ACCESS_KEY_PAIRS__2__SECRET_KEY': 'incoming-some-secret-2',
        'INCOMING_ACCESS_KEY_PAIRS__2__PERMISSIONS__1': 'POST',
        'INCOMING_ACCESS_KEY_PAIRS__3__KEY_ID': 'incoming-some-id-3',
        'INCOMING_ACCESS_KEY_PAIRS__3__SECRET_KEY': 'incoming-some-secret-3',
        'INCOMING_ACCESS_KEY_PAIRS__3__PERMISSIONS__1': 'GET',
        'INCOMING_IP_WHITELIST__1': '1.2.3.4',
        'INCOMING_IP_WHITELIST__2': '2.3.4.5',
    }
