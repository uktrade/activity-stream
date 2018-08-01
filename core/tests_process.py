import asyncio
import os
from subprocess import Popen, PIPE
import sys
import unittest
from unittest.mock import Mock

from aiohttp import web

from core.tests_utils import (
    async_test,
    delete_all_es_data,
    is_http_accepted_eventually,
    mock_env,
    read_file,
    run_es_application,
    run_feed_application,
)


class TestProcess(unittest.TestCase):

    def add_async_cleanup(self, coroutine):
        loop = asyncio.get_event_loop()
        self.addCleanup(loop.run_until_complete, coroutine())

    async def terminate_with_output(self, server):
        await asyncio.sleep(1)
        server.terminate()
        # PaaS has 10 seconds to exit cleanly
        await asyncio.sleep(1)
        output, _ = server.communicate()
        return output

    async def setup_manual(self, env):
        await delete_all_es_data()

        feed_runner_1 = await run_feed_application(read_file, lambda: 200, Mock(), 8081)
        server = Popen([sys.executable, '-m', 'core.app_outgoing'], env={
            **env,
            'COVERAGE_PROCESS_START': os.environ['COVERAGE_PROCESS_START'],
        }, stdout=PIPE)

        async def tear_down():
            server.terminate()
            await feed_runner_1.cleanup()

        self.add_async_cleanup(tear_down)
        return server

    @async_test
    async def test_http_and_exit_clean(self):
        server = await self.setup_manual(mock_env())
        self.assertTrue(await is_http_accepted_eventually())

        output = await self.terminate_with_output(server)

        final_string = b'Reached end of main. Exiting now.\n'
        self.assertEqual(final_string, output[-len(final_string):])
        self.assertEqual(server.returncode, 0)

    @async_test
    async def test_if_es_down_exit_clean(self):
        server = await self.setup_manual({
            **mock_env(), 'ELASTICSEARCH__PORT': '9202'  # Nothing listening
        })
        self.assertTrue(await is_http_accepted_eventually())

        output = await self.terminate_with_output(server)

        final_string = b'Reached end of main. Exiting now.\n'
        self.assertEqual(final_string, output[-len(final_string):])
        self.assertEqual(server.returncode, 0)

    @async_test
    async def test_if_es_slow_exit_clean(self):
        async def return_200_slow(_):
            await asyncio.sleep(30)

        routes = [
            web.post('/_bulk', return_200_slow),
        ]
        es_runner = await run_es_application(port=9201, override_routes=routes)
        self.add_async_cleanup(es_runner.cleanup)

        server = await self.setup_manual({
            **mock_env(), 'ELASTICSEARCH__PORT': '9201'
        })
        self.assertTrue(await is_http_accepted_eventually())

        output = await self.terminate_with_output(server)

        final_string = b'Reached end of main. Exiting now.\n'
        self.assertEqual(final_string, output[-len(final_string):])
        self.assertEqual(server.returncode, 0)
