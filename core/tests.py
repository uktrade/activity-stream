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


class TestBase(unittest.TestCase):

    def setup_manual(self, env, feed, es_bulk):
        ''' Test setUp function that can be customised on a per-test basis '''
        self.os_environ_patcher = patch.dict(os.environ, {
            **mock_env(),
            **env,
        })
        self.os_environ_patcher.start()
        self.loop = asyncio.get_event_loop()

        def es_bulk_callback(result):
            first_not_done = next(future for future in es_bulk if not future.done())
            first_not_done.set_result(result)

        self.feed_requested = [asyncio.Future(), asyncio.Future()]

        def feed_requested_callback(request):
            first_not_done = next(future for future in self.feed_requested if not future.done())
            first_not_done.set_result(request)

        self.es_runner, self.feed_runner_1 = \
            self.loop.run_until_complete(asyncio.gather(
                run_es_application(es_bulk_callback),
                run_feed_application(feed, feed_requested_callback, 8081),
            ))

        original_app_runner = aiohttp.web.AppRunner

        def wrapped_app_runner(*args, **kwargs):
            self.app_runner = original_app_runner(*args, **kwargs)
            return self.app_runner

        self.app_runner_patcher = patch('aiohttp.web.AppRunner', wraps=wrapped_app_runner)
        self.app_runner_patcher.start()

    def tearDown(self):
        for task in asyncio.Task.all_tasks():
            task.cancel()
        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(asyncio.gather(
            self.app_runner.cleanup(),
            self.feed_runner_1.cleanup(),
            self.es_runner.cleanup(),
        ))
        self.app_runner_patcher.stop()
        self.os_environ_patcher.stop()


class TestConnection(TestBase):

    def test_application_accepts_http(self):
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        asyncio.ensure_future(run_application(), loop=self.loop)
        self.assertTrue(is_http_accepted_eventually())


class TestAuthentication(TestBase):

    def test_no_auth_then_401(self):
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        asyncio.ensure_future(run_application(), loop=self.loop)
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/'
        text, status = self.loop.run_until_complete(post_text_no_auth(url, '1.2.3.4'))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Authentication credentials were not provided."}')

    def test_bad_id_then_401(self):
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        asyncio.ensure_future(run_application(), loop=self.loop)
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/'
        auth = auth_header(
            'incoming-some-id-incorrect', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(post_text(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_bad_secret_then_401(self):
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        asyncio.ensure_future(run_application(), loop=self.loop)
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/'
        auth = auth_header(
            'incoming-some-id-1', 'incoming-some-secret-2', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(post_text(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_bad_method_then_401(self):
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        asyncio.ensure_future(run_application(), loop=self.loop)
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/'
        auth = auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'GET', '', '',
        )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(post_text(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_bad_content_then_401(self):
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        asyncio.ensure_future(run_application(), loop=self.loop)
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/'
        auth = auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', 'content', '',
        )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(post_text(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_bad_content_type_then_401(self):
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        asyncio.ensure_future(run_application(), loop=self.loop)
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/'
        auth = auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', 'some-type',
        )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(post_text(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_no_content_type_then_401(self):
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        asyncio.ensure_future(run_application(), loop=self.loop)
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/'
        auth = auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', 'some-type',
        )
        x_forwarded_for = '1.2.3.4'
        _, status = self.loop.run_until_complete(
            post_text_no_content_type(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)

    def test_time_skew_then_401(self):
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        asyncio.ensure_future(run_application(), loop=self.loop)
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/'
        past = datetime.datetime.now() + datetime.timedelta(seconds=-61)
        with freeze_time(past):
            auth = auth_header(
                'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
            )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(post_text(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_repeat_auth_then_401(self):
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        asyncio.ensure_future(run_application(), loop=self.loop)
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/'
        auth = auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4'
        _, status_1 = self.loop.run_until_complete(post_text(url, auth, x_forwarded_for))
        self.assertEqual(status_1, 200)

        text_2, status_2 = self.loop.run_until_complete(post_text(url, auth, x_forwarded_for))
        self.assertEqual(status_2, 401)
        self.assertEqual(text_2, '{"details": "Incorrect authentication credentials."}')

    def test_nonces_cleared(self):
        ''' Makes duplicate requests, but with the code patched so the nonce expiry time
            is shorter then the allowed Hawk skew. The second request succeeding gives
            evidence that the cache of nonces was cleared.
        '''
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        now = datetime.datetime.now()
        past = now + datetime.timedelta(seconds=-45)

        with patch('core.app.NONCE_EXPIRE', 30):
            asyncio.ensure_future(run_application(), loop=self.loop)
            is_http_accepted_eventually()

            url = 'http://127.0.0.1:8080/'
            x_forwarded_for = '1.2.3.4'

            with freeze_time(past):
                auth = auth_header(
                    'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
                )
                _, status_1 = self.loop.run_until_complete(
                    post_text(url, auth, x_forwarded_for))
            self.assertEqual(status_1, 200)

            with freeze_time(now):
                _, status_2 = self.loop.run_until_complete(
                    post_text(url, auth, x_forwarded_for))
            self.assertEqual(status_2, 200)

    def test_no_x_forwarded_for_401(self):
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        asyncio.ensure_future(run_application(), loop=self.loop)
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/'
        auth = auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        text, status = self.loop.run_until_complete(post_text_no_x_forwarded_for(url, auth))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_bad_x_forwarded_for_401(self):
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        asyncio.ensure_future(run_application(), loop=self.loop)
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/'
        auth = auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        x_forwarded_for = '3.4.5.6'
        text, status = self.loop.run_until_complete(post_text(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_at_end_x_forwarded_for_401(self):
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        asyncio.ensure_future(run_application(), loop=self.loop)
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/'
        auth = auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        x_forwarded_for = '3.4.5.6,1.2.3.4'
        text, status = self.loop.run_until_complete(post_text(url, auth, x_forwarded_for))
        self.assertEqual(status, 401)
        self.assertEqual(text, '{"details": "Incorrect authentication credentials."}')

    def test_second_id_returns_object(self):
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        asyncio.ensure_future(run_application(), loop=self.loop)
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/'
        auth = auth_header(
            'incoming-some-id-2', 'incoming-some-secret-2', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(post_text(url, auth, x_forwarded_for))
        self.assertEqual(status, 200)
        self.assertEqual(text, '{"secret": "to-be-hidden"}')

    def test_post_returns_object(self):
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        asyncio.ensure_future(run_application(), loop=self.loop)
        is_http_accepted_eventually()

        url = 'http://127.0.0.1:8080/'
        auth = auth_header(
            'incoming-some-id-1', 'incoming-some-secret-1', url, 'POST', '', '',
        )
        x_forwarded_for = '1.2.3.4'
        text, status = self.loop.run_until_complete(post_text(url, auth, x_forwarded_for))
        self.assertEqual(status, 200)
        self.assertEqual(text, '{"secret": "to-be-hidden"}')


class TestApplication(TestBase):

    @freeze_time('2012-01-14 12:00:01')
    @patch('os.urandom', return_value=b'something-random')
    def test_single_page(self, _):
        es_bulk = [asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed,
            es_bulk,
        )

        async def _test():
            asyncio.ensure_future(run_application())
            return await es_bulk[0]

        es_bulk_content, es_bulk_headers = self.loop.run_until_complete(_test())
        es_bulk_request_dicts = [
            json.loads(line)
            for line in es_bulk_content.split(b'\n')[0:-1]
        ]

        self.assertEqual(self.feed_requested[0].result(
        ).headers['Authorization'], (
            'Hawk '
            'mac="yK3tQ9t/2/lJjCzyQ8pLoEU6M8RXzVt/yWQRPmSCy7Q=", '
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
            'Signature=544dff75ed37c19a96124d849cd09bd1488c061dc1666fedf93d5bc20609d78b')
        self.assertEqual(es_bulk_content.decode('utf-8')[-1], '\n')
        self.assertEqual(es_bulk_headers['Content-Type'], 'application/x-ndjson')

        self.assertEqual(es_bulk_request_dicts[0]['index']['_index'], 'company_timeline')
        self.assertEqual(es_bulk_request_dicts[0]['index']['_type'], '_doc')
        self.assertEqual(es_bulk_request_dicts[0]['index']
                         ['_id'], 'export-oportunity-enquiry-made-49863')
        self.assertEqual(es_bulk_request_dicts[1]['date'], '2018-04-12T12:48:13+00:00')
        self.assertEqual(es_bulk_request_dicts[1]['activity'], 'export-oportunity-enquiry-made')
        self.assertEqual(es_bulk_request_dicts[1]['company_house_number'], '123432')

        self.assertEqual(es_bulk_request_dicts[2]['index']['_index'], 'company_timeline')
        self.assertEqual(es_bulk_request_dicts[2]['index']['_type'], '_doc')
        self.assertEqual(es_bulk_request_dicts[2]['index']
                         ['_id'], 'export-oportunity-enquiry-made-49862')
        self.assertEqual(es_bulk_request_dicts[3]['date'], '2018-03-23T17:06:53+00:00')
        self.assertEqual(es_bulk_request_dicts[3]['activity'], 'export-oportunity-enquiry-made')
        self.assertEqual(es_bulk_request_dicts[3]['company_house_number'], '82312')

    def test_multipage(self):
        es_bulk = [asyncio.Future(), asyncio.Future()]
        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_multipage_1.json'},
            mock_feed,
            es_bulk,
        )

        async def _test():
            asyncio.ensure_future(run_application())
            return await es_bulk[1]

        es_bulk_content, _ = self.loop.run_until_complete(_test())

        es_bulk_request_dicts = [
            json.loads(line)
            for line in es_bulk_content.split(b'\n')[0:-1]
        ]
        self.assertEqual(es_bulk_request_dicts[0]['index']['_id'],
                         'export-oportunity-enquiry-made-second-page-4986999')

    def test_two_feeds(self):
        es_bulk = [asyncio.Future(), asyncio.Future()]
        self.setup_manual(
            {
                'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json',
                'FEED_ENDPOINTS__2__URL': 'http://localhost:8081/tests_fixture_2.json',
            },
            mock_feed,
            es_bulk,
        )

        async def _test():
            asyncio.ensure_future(run_application())
            return await asyncio.gather(es_bulk[0], es_bulk[1])

        es_1, es_2 = self.loop.run_until_complete(_test())
        es_bulk_content_1, _ = es_1
        es_bulk_content_2, _ = es_2

        es_bulk_request_dicts_1 = [
            json.loads(line)
            for line in es_bulk_content_1.split(b'\n')[0:-1]
        ]
        es_bulk_request_dicts_2 = [
            json.loads(line)
            for line in es_bulk_content_2.split(b'\n')[0:-1]
        ]
        ids = [
            es_bulk_request_dicts_1[0]['index']['_id'],
            es_bulk_request_dicts_2[0]['index']['_id'],
        ]
        self.assertIn('export-oportunity-enquiry-made-49863', ids)
        self.assertIn('export-oportunity-enquiry-made-42863', ids)

    def test_on_bad_json_retries(self):
        es_bulk = [asyncio.Future(), asyncio.Future()]

        sent_broken = False

        def mock_feed_broken_then_fixed(path):
            nonlocal sent_broken

            feed_contents_maybe_broken = (
                mock_feed(path) +
                ('something-invalid' if not sent_broken else '')
            )
            sent_broken = True
            return feed_contents_maybe_broken

        self.setup_manual(
            {'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'},
            mock_feed_broken_then_fixed,
            es_bulk,
        )

        original_sleep = asyncio.sleep

        async def fast_sleep(_):
            await original_sleep(0)

        async def _test():
            with patch('asyncio.sleep', wraps=fast_sleep) as mock_sleep:
                asyncio.ensure_future(run_application())
                mock_sleep.assert_not_called()
                result = await es_bulk[0]
                mock_sleep.assert_called_once_with(60)
                return result

        es_bulk_content, _ = self.loop.run_until_complete(_test())

        es_bulk_request_dicts = [
            json.loads(line)
            for line in es_bulk_content.split(b'\n')[0:-1]
        ]
        self.assertIn(
            'export-oportunity-enquiry-made-49863',
            es_bulk_request_dicts[0]['index']['_id'],
        )


class TestProcess(unittest.TestCase):

    def setUp(self):
        loop = asyncio.get_event_loop()

        self.feed_runner_1 = loop.run_until_complete(run_feed_application(mock_feed, Mock(), 8081))
        self.es_runner = loop.run_until_complete(run_es_application(Mock()))
        self.server = Popen([sys.executable, '-m', 'core.app'], env={
            **mock_env(),
            **{'FEED_ENDPOINTS__1__URL': 'http://localhost:8081/tests_fixture_1.json'}
        })

    def tearDown(self):
        for task in asyncio.Task.all_tasks():
            task.cancel()
        self.server.kill()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.feed_runner_1.cleanup())
        loop.run_until_complete(self.es_runner.cleanup())

    def test_server_accepts_http(self):
        self.assertTrue(is_http_accepted_eventually())


def is_http_accepted_eventually():
    loop = asyncio.get_event_loop()
    connected_future = asyncio.ensure_future(_is_http_accepted_eventually(), loop=loop)
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


def mock_feed(path):
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


async def run_es_application(es_bulk_request_callback):
    async def handle(request):
        content, headers = (await request.content.read(), request.headers)
        asyncio.get_event_loop().call_soon(es_bulk_request_callback, (content, headers))
        return web.Response(text='')

    app = web.Application()
    app.add_routes([web.post('/_bulk', handle)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 8082)
    await site.start()
    return runner


def auth_header(key_id, secret_key, url, method, content, content_type):
    return mohawk.Sender({
        'id': key_id,
        'key': secret_key,
        'algorithm': 'sha256',
    }, url, method, content=content, content_type=content_type).request_header


async def post_text(url, auth, x_forwarded_for):
    async with aiohttp.ClientSession() as session:
        result = await session.post(url, headers={
            'Authorization': auth,
            'Content-Type': '',
            'X-Forwarded-For': x_forwarded_for,
        }, timeout=1)
    return (await result.text(), result.status)


async def post_text_no_auth(url, x_forwarded_for):
    async with aiohttp.ClientSession() as session:
        result = await session.post(url, headers={
            'Content-Type': '',
            'X-Forwarded-For': x_forwarded_for,
        }, timeout=1)
    return (await result.text(), result.status)


async def post_text_no_x_forwarded_for(url, auth):
    async with aiohttp.ClientSession() as session:
        headers = {
            'Authorization': auth,
            'Content-Type': '',
        }
        result = await session.post(url, headers=headers, timeout=1)
    return (await result.text(), result.status)


async def post_text_no_content_type(url, auth, x_forwarded_for):
    async with aiohttp.ClientSession() as session:
        result = await session.post(url, headers={
            'Authorization': auth,
            'X-Forwarded-For': x_forwarded_for,
        }, timeout=1)

    return (await result.text(), result.status)


def mock_env():
    return {
        'PORT': '8080',
        'ELASTICSEARCH_AWS_ACCESS_KEY_ID': 'some-id',
        'ELASTICSEARCH_AWS_SECRET_ACCESS_KEY': 'aws-secret',
        'ELASTICSEARCH_HOST': '127.0.0.1',
        'ELASTICSEARCH_PORT': '8082',
        'ELASTICSEARCH_PROTOCOL': 'http',
        'ELASTICSEARCH_REGION': 'us-east-2',
        'FEED_ACCESS_KEY_ID': 'feed-some-id',
        'FEED_SECRET_ACCESS_KEY': '?[!@$%^%',
        'INCOMING_ACCESS_KEY_PAIRS__1__KEY_ID': 'incoming-some-id-1',
        'INCOMING_ACCESS_KEY_PAIRS__1__SECRET_KEY': 'incoming-some-secret-1',
        'INCOMING_ACCESS_KEY_PAIRS__2__KEY_ID': 'incoming-some-id-2',
        'INCOMING_ACCESS_KEY_PAIRS__2__SECRET_KEY': 'incoming-some-secret-2',
        'INCOMING_IP_WHITELIST__1': '1.2.3.4',
        'INCOMING_IP_WHITELIST__2': '2.3.4.5',
    }
