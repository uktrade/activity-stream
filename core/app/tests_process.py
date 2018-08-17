import asyncio
import os
import sys
import unittest
from unittest.mock import Mock

from aiohttp import web

from .tests_utils import (
    async_test,
    delete_all_es_data,
    delete_all_redis_data,
    is_http_accepted_eventually,
    wait_until_get_working,
    has_at_least_ordered_items,
    get_until,
    get_until_raw,
    mock_env,
    read_file,
    run_es_application,
    run_feed_application,
)


class TestProcess(unittest.TestCase):

    def add_async_cleanup(self, coroutine):
        loop = asyncio.get_event_loop()
        self.addCleanup(loop.run_until_complete, coroutine())

    async def terminate(self, server_out, server_inc):
        await asyncio.sleep(1)
        server_out.terminate()
        server_inc.terminate()
        await server_out.wait()
        await server_inc.wait()
        # PaaS has 10 seconds to exit cleanly
        await asyncio.sleep(1)

    async def setup_manual(self, common_env):
        await delete_all_es_data()
        await delete_all_redis_data()

        env = {
            **common_env,
            'COVERAGE_PROCESS_START': os.environ['COVERAGE_PROCESS_START'],
            'FEEDS__2__UNIQUE_ID': 'verification',
            'FEEDS__2__SEED': 'http://localhost:8082/0',
            'FEEDS__2__ACCESS_KEY_ID': '',
            'FEEDS__2__SECRET_ACCESS_KEY': '',
            'FEEDS__2__TYPE': 'activity_stream',
        }
        feed_runner_1 = await run_feed_application(read_file, lambda: 200, Mock(), 8081)
        feed_runner_2 = await asyncio.create_subprocess_exec(
            *[sys.executable, '-m', 'verification_feed.app'], env={
                **env,
                'PORT': '8082',
            }, stdout=asyncio.subprocess.PIPE)
        server_out = await asyncio.create_subprocess_exec(
            *[sys.executable, '-m', 'core.app.app_outgoing'],
            env=env, stdout=asyncio.subprocess.PIPE)
        server_inc = await asyncio.create_subprocess_exec(
            *[sys.executable, '-m', 'core.app.app_incoming'],
            env=env, stdout=asyncio.subprocess.PIPE)

        stdout_out = [None]
        stdout_inc = [None]
        is_running = True

        async def populate_stdout(process, stdout):
            while is_running:
                output = await process.stdout.readline()
                if output != b'':
                    stdout[0] = output
                await asyncio.sleep(0)

        asyncio.get_event_loop().create_task(populate_stdout(server_out, stdout_out))
        asyncio.get_event_loop().create_task(populate_stdout(server_inc, stdout_inc))

        async def tear_down():
            nonlocal is_running
            is_running = False
            await asyncio.sleep(0.2)
            if server_out.returncode is None:
                server_out.kill()
            await server_out.wait()
            if server_inc.returncode is None:
                server_inc.kill()
            await server_inc.wait()
            if feed_runner_2.returncode is None:
                feed_runner_2.kill()
            await feed_runner_2.wait()
            await feed_runner_1.cleanup()
            await asyncio.sleep(1)

        def get_server_out_stdout():
            return stdout_out[0]

        def get_server_inc_stdout():
            return stdout_inc[0]

        self.add_async_cleanup(tear_down)
        return \
            (server_out, get_server_out_stdout), \
            (server_inc, get_server_inc_stdout), \
            feed_runner_2,

    @async_test
    async def test_http_and_exit_clean(self):
        (server_out, stdout_out), (server_inc, stdout_inc), _ = await self.setup_manual(mock_env())
        self.assertTrue(await is_http_accepted_eventually())
        await wait_until_get_working()

        url = 'http://127.0.0.1:8080/v1/'
        x_forwarded_for = '1.2.3.4, 127.0.0.0'
        result, _, _ = await get_until(url, x_forwarded_for,
                                       has_at_least_ordered_items(500))
        ids = [item['id'] for item in result['orderedItems']]
        self.assertIn('dit:activityStreamVerificationFeed:Verifier', str(ids))

        await self.terminate(server_out, server_inc)

        final_string = b'Reached end of main. Exiting now.\n'
        self.assertEqual(final_string, stdout_inc())
        self.assertEqual(final_string, stdout_out())
        self.assertEqual(server_inc.returncode, 0)
        self.assertEqual(server_out.returncode, 0)

    @async_test
    async def test_if_es_down_exit_clean(self):
        (server_out, stdout_out), (server_inc, stdout_inc), _ = await self.setup_manual({
            **mock_env(), 'ELASTICSEARCH__PORT': '9202'  # Nothing listening
        })
        self.assertTrue(await is_http_accepted_eventually())

        await self.terminate(server_out, server_inc)

        final_string = b'Reached end of main. Exiting now.\n'
        self.assertEqual(final_string, stdout_inc())
        self.assertEqual(final_string, stdout_out())
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

        (server_out, stdout_out), (server_inc, stdout_inc), _ = await self.setup_manual({
            **mock_env(), 'ELASTICSEARCH__PORT': '9201'
        })
        self.assertTrue(await is_http_accepted_eventually())

        await self.terminate(server_out, server_inc)

        final_string = b'Reached end of main. Exiting now.\n'
        self.assertEqual(final_string, stdout_inc())
        self.assertEqual(final_string, stdout_out())
        self.assertEqual(server_inc.returncode, 0)
        self.assertEqual(server_out.returncode, 0)

    @async_test
    async def test_metrics_and_check(self):
        _, _, verification_feed = await self.setup_manual(mock_env())
        self.assertTrue(await is_http_accepted_eventually())
        await wait_until_get_working()

        x_forwarded_for = '1.2.3.4, 127.0.0.0'

        def metrics_has_success(text):
            return 'status="success"' in text and \
                   'elasticsearch_activities_age_minimum_seconds' \
                   '{feed_unique_id="verification"}' in text

        metrics_url = 'http://127.0.0.1:8080/metrics'
        metrics, _, _ = await get_until_raw(metrics_url, x_forwarded_for, metrics_has_success)
        self.assertIn('elasticsearch_activities_age_minimum_seconds'
                      '{feed_unique_id="verification"}', metrics)

        def check_is_up(text):
            return '__UP__' in text

        check_url = 'http://127.0.0.1:8080/check'
        check, _, _ = await get_until_raw(check_url, x_forwarded_for, check_is_up)
        self.assertIn('__UP__', check)

        def check_verification_is_green(text):
            return 'verification:GREEN' in text
        _, _, _ = await get_until_raw(check_url, x_forwarded_for,
                                      check_verification_is_green)

        def check_is_not_up(text):
            return '__UP__' not in text

        verification_feed.terminate()
        check_down, _, _ = await get_until_raw(check_url, x_forwarded_for, check_is_not_up)
        self.assertIn('__DOWN__', check_down)
