import asyncio
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
        self.os_environ_patcher.stop()

    def test_application_accepts_http(self):
        asyncio.ensure_future(run_application(), loop=self.loop)
        self.assertTrue(is_http_accepted())

    def test_feed_printed(self):
        async def _test():
            asyncio.ensure_future(run_application())
            await self.feed_requested

        with patch('core.app.print') as mock_print:
            self.loop.run_until_complete(_test())
            mock_print.assert_called_with(b'feed-contents')


class TestProcess(unittest.TestCase):

    def setUp(self):
        loop = asyncio.get_event_loop()

        self.feed_runner = loop.run_until_complete(run_feed_application(Mock()))
        self.server = Popen([sys.executable, '-m', 'core.app'], env=mock_env())

    def tearDown(self):
        for task in asyncio.Task.all_tasks():
            task.cancel()
        self.server.kill()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.feed_runner.cleanup())

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
        return web.Response(text='feed-contents')

    app = web.Application()
    app.add_routes([web.get('/feed', handle)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', 8081)
    await site.start()
    return runner


def mock_env():
    return {
        'FEED_ENDPOINT': 'http://localhost:8081/feed',
        'INTERNAL_API_SHARED_SECRET': 'some-secret'
    }
