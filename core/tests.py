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

    def setUp(self):
        self.os_environ_patcher = patch.dict(os.environ, mock_env())
        self.os_environ_patcher.start()
        self.loop = asyncio.get_event_loop()

        self.es_bulk = asyncio.Future()

        def es_bulk_callback(result):
            self.es_bulk.set_result(result)
        self.es_runner = self.loop.run_until_complete(
            run_es_application(es_bulk_callback))

        self.feed_requested = asyncio.Future()

        def feed_requested_callback():
            self.feed_requested.set_result(None)
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
        asyncio.ensure_future(run_application(), loop=self.loop)
        self.assertTrue(is_http_accepted())

    @freeze_time('2012-01-14 12:00:01')
    def test_feed_passed_to_elastic_search(self):
        async def _test():
            asyncio.ensure_future(run_application())
            return await self.es_bulk

        es_bulk_content, es_bulk_headers = self.loop.run_until_complete(_test())
        es_bulk_request_dicts = [
            json.loads(line)
            for line in es_bulk_content.split(b'\n')[0:-1]
        ]

        self.assertEqual(
            es_bulk_headers['Authorization'],
            'AWS4-HMAC-SHA256 '
            'Credential=some-id/20120114/us-east-2/es/aws4_request, '
            'SignedHeaders=content-type;host;x-amz-date, '
            'Signature=2491bc4f0759767e13154defae392ab2fa45833393424a5d3d34370bc7842255')
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


class TestProcess(unittest.TestCase):

    def setUp(self):
        loop = asyncio.get_event_loop()

        self.feed_runner = loop.run_until_complete(run_feed_application(Mock()))
        self.es_runner = loop.run_until_complete(run_es_application(Mock()))
        self.server = Popen([sys.executable, '-m', 'core.app'], env=mock_env())

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
    async def handle(request):
        asyncio.get_event_loop().call_soon(feed_requested_callback)
        return web.Response(text=mock_feed())

    app = web.Application()
    app.add_routes([web.get('/feed', handle)])
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
        'FEED_ENDPOINT': 'http://localhost:8081/feed',
        'ELASTICSEARCH_AWS_ACCESS_KEY_ID': 'some-id',
        'ELASTICSEARCH_AWS_SECRET_ACCESS_KEY': 'aws-secret',
        'ELASTICSEARCH_HOST': '127.0.0.1',
        'ELASTICSEARCH_PORT': '8082',
        'ELASTICSEARCH_PROTOCOL': 'http',
        'ELASTICSEARCH_REGION': 'us-east-2',
        'INTERNAL_API_SHARED_SECRET': 'some-secret'
    }


def mock_feed():
    with open('core/tests_fixture.xml', 'rb') as f:
        return f.read().decode('utf-8')
