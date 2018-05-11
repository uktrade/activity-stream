import asyncio
from subprocess import Popen
import unittest

import aiohttp


class TestProcess(unittest.TestCase):

    def setUp(self):
        self.server = Popen(['python', '-m', 'core.app'])

    def tearDown(self):
        self.server.kill()

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
