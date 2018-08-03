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
    wait_until_get_working,
    has_at_least_ordered_items,
    get_until,
    mock_env,
    read_file,
    run_es_application,
    run_feed_application,
)


class TestProcess(unittest.TestCase):

    def add_async_cleanup(self, coroutine):
        loop = asyncio.get_event_loop()
        self.addCleanup(loop.run_until_complete, coroutine())

    async def terminate_with_output(self, server_out, server_inc):
        await asyncio.sleep(1)
        server_out.terminate()
        server_inc.terminate()
        # PaaS has 10 seconds to exit cleanly
        await asyncio.sleep(1)
        output_out, _ = server_out.communicate()
        output_inc, _ = server_inc.communicate()
        return output_out, output_inc

    async def setup_manual(self, common_env):
        await delete_all_es_data()

        env = {
            **common_env,
            'COVERAGE_PROCESS_START': os.environ['COVERAGE_PROCESS_START'],
            'FEEDS__2__UNIQUE_ID': 'verification',
            'FEEDS__2__SEED': 'http://localhost:8082/',
            'FEEDS__2__ACCESS_KEY_ID': '',
            'FEEDS__2__SECRET_ACCESS_KEY': '',
            'FEEDS__2__TYPE': 'activity_stream',
        }
        feed_runner_1 = await run_feed_application(read_file, lambda: 200, Mock(), 8081)
        feed_runner_2 = Popen([sys.executable, '-m', 'verification_feed.app'], env={
            **env,
            'PORT': '8082',
        }, stdout=PIPE)
        server_out = Popen([sys.executable, '-m', 'core.app_outgoing'], env=env, stdout=PIPE)
        server_inc = Popen([sys.executable, '-m', 'core.app_incoming'], env=env, stdout=PIPE)

        async def tear_down():
            server_inc.terminate()
            server_out.terminate()
            feed_runner_2.terminate()
            await feed_runner_1.cleanup()

        self.add_async_cleanup(tear_down)
        return server_out, server_inc

    @async_test
    async def test_http_and_exit_clean(self):
        server_out, server_inc = await self.setup_manual(mock_env())
        self.assertTrue(await is_http_accepted_eventually())
        await wait_until_get_working()

        url = 'http://127.0.0.1:8080/v1/'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        result, _, _ = await get_until(url, x_forwarded_for,
                                       has_at_least_ordered_items(3), asyncio.sleep)
        self.assertEqual(result['orderedItems'][0]['id'],
                         'dit:exportOpportunities:Enquiry:49863:Create')
        self.assertEqual(result['orderedItems'][2]['id'],
                         'dit:activityStreamVerificationFeed:Verifier:1:Create')

        output_out, output_inc = await self.terminate_with_output(server_out, server_inc)

        final_string = b'Reached end of main. Exiting now.\n'
        self.assertEqual(final_string, output_inc[-len(final_string):])
        self.assertEqual(final_string, output_out[-len(final_string):])
        self.assertEqual(server_inc.returncode, 0)
        self.assertEqual(server_out.returncode, 0)

    @async_test
    async def test_if_es_down_exit_clean(self):
        server_out, server_inc = await self.setup_manual({
            **mock_env(), 'ELASTICSEARCH__PORT': '9202'  # Nothing listening
        })
        self.assertTrue(await is_http_accepted_eventually())

        output_out, output_inc = await self.terminate_with_output(server_out, server_inc)

        final_string = b'Reached end of main. Exiting now.\n'
        self.assertEqual(final_string, output_inc[-len(final_string):])
        self.assertEqual(final_string, output_out[-len(final_string):])
        self.assertEqual(server_inc.returncode, 0)
        self.assertEqual(server_out.returncode, 0)

    @async_test
    async def test_if_es_slow_exit_clean(self):
        async def return_200_slow(_):
            await asyncio.sleep(30)

        routes = [
            web.post('/_bulk', return_200_slow),
        ]
        es_runner = await run_es_application(port=9201, override_routes=routes)
        self.add_async_cleanup(es_runner.cleanup)

        server_out, server_inc = await self.setup_manual({
            **mock_env(), 'ELASTICSEARCH__PORT': '9201'
        })
        self.assertTrue(await is_http_accepted_eventually())

        output_out, output_inc = await self.terminate_with_output(server_out, server_inc)

        final_string = b'Reached end of main. Exiting now.\n'
        self.assertEqual(final_string, output_inc[-len(final_string):])
        self.assertEqual(final_string, output_out[-len(final_string):])
        self.assertEqual(server_inc.returncode, 0)
        self.assertEqual(server_out.returncode, 0)
