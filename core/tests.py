import asyncio
from freezegun import freeze_time
import json
import os
from subprocess import Popen
import sys
import unittest
from unittest.mock import Mock, patch

import aiohttp
from aiohttp import web

from core.app import run_application


class TestApplication(unittest.TestCase):

    def setUp_manual(self, env):
        ''' Test setUp function that can be customised on a per-test basis '''
        self.os_environ_patcher = patch.dict(os.environ, {
            **mock_env(),
            **env,
        })
        self.os_environ_patcher.start()
        self.loop = asyncio.get_event_loop()

        self.es_bulk = [asyncio.Future(), asyncio.Future()]

        def es_bulk_callback(result):
            first_not_done = next(future for future in self.es_bulk if not future.done())
            first_not_done.set_result(result)

        self.es_runner = self.loop.run_until_complete(
            run_es_application(es_bulk_callback))

        self.feed_requested = [asyncio.Future(), asyncio.Future()]

        def feed_requested_callback(request):
            first_not_done = next(future for future in self.feed_requested if not future.done())
            first_not_done.set_result(request)
        self.feed_runner = self.loop.run_until_complete(
            run_feed_application(feed_requested_callback))

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
        self.loop.run_until_complete(self.app_runner.cleanup())
        self.app_runner_patcher.stop()
        self.loop.run_until_complete(self.feed_runner.cleanup())
        self.loop.run_until_complete(self.es_runner.cleanup())
        self.os_environ_patcher.stop()

    def test_application_accepts_http(self):
        self.setUp_manual({'FEED_ENDPOINT': 'http://localhost:8081/tests_fixture.xml'})

        asyncio.ensure_future(run_application(), loop=self.loop)
        self.assertTrue(is_http_accepted())

    @freeze_time('2012-01-14 12:00:01')
    @patch('os.urandom', return_value=b'something-random')
    def test_feed_passed_to_elastic_search(self, _):
        self.setUp_manual({'FEED_ENDPOINT': 'http://localhost:8081/tests_fixture.xml'})

        async def _test():
            asyncio.ensure_future(run_application())
            return await self.es_bulk[0]

        es_bulk_content, es_bulk_headers = self.loop.run_until_complete(_test())
        es_bulk_request_dicts = [
            json.loads(line)
            for line in es_bulk_content.split(b'\n')[0:-1]
        ]

        self.assertEqual(self.feed_requested[0].result(
        ).headers['Authorization'],
            'Hawk '
            'mac="TkV+IxaD2wp00lNY1adIVzGrmUEa8cSE7AcAoswXjzU=", '
            'hash="B0weSUXsMcb5UhL41FZbrUJCAotzSI3HawE1NPLRUz8=", '
            'id="feed-some-id", '
            'ts="1326542401", '
            'nonce="c29tZX"'
        )

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

    def test_multipage_second_page_passed_to_elastic_search(self):
        self.setUp_manual({'FEED_ENDPOINT': 'http://localhost:8081/tests_fixture_multipage_1.xml'})

        async def _test():
            asyncio.ensure_future(run_application())
            return await self.es_bulk[1]

        es_bulk_content, es_bulk_headers = self.loop.run_until_complete(_test())

        es_bulk_request_dicts = [
            json.loads(line)
            for line in es_bulk_content.split(b'\n')[0:-1]
        ]
        self.assertEqual(es_bulk_request_dicts[0]['index']['_id'],
                         'export-oportunity-enquiry-made-second-page-4986999')


class TestProcess(unittest.TestCase):

    def setUp(self):
        loop = asyncio.get_event_loop()

        self.feed_runner = loop.run_until_complete(run_feed_application(Mock()))
        self.es_runner = loop.run_until_complete(run_es_application(Mock()))
        self.server = Popen([sys.executable, '-m', 'core.app'], env={
            **mock_env(),
            **{'FEED_ENDPOINT': 'http://localhost:8081/tests_fixture.xml'}
        })

    def tearDown(self):
        for task in asyncio.Task.all_tasks():
            task.cancel()
        self.server.kill()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.feed_runner.cleanup())
        loop.run_until_complete(self.es_runner.cleanup())

    def test_server_accepts_http(self):
        self.assertTrue(is_http_accepted())


def is_http_accepted():
    loop = asyncio.get_event_loop()
    connected_future = asyncio.ensure_future(_is_http_accepted(), loop=loop)
    return loop.run_until_complete(connected_future)


async def _is_http_accepted():
    def is_connection_error(e):
        return 'Cannot connect to host' in str(e)

    attempts = 0
    while attempts < 20:
        try:
            async with aiohttp.ClientSession() as session:
                await session.get('http://127.0.0.1:8080', timeout=1)
            return True
        except aiohttp.client_exceptions.ClientConnectorError as e:
            attempts += 1
            await asyncio.sleep(0.2)
            if not is_connection_error(e):
                return True

    return False


async def run_feed_application(feed_requested_callback):
    def mock_feed(path):
        with open('core/' + path, 'rb') as f:
            return f.read().decode('utf-8')

    async def handle(request):
        path = request.match_info['feed']
        asyncio.get_event_loop().call_soon(feed_requested_callback, request)
        return web.Response(text=mock_feed(path))

    app = web.Application()
    app.add_routes([web.get('/{feed}', handle)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 8081)
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
    }
