import asyncio
from datetime import (
    datetime,
    timezone,
)
import logging
import os
import sys

from aiohttp import web


LOGGER_NAME = 'activity-stream-verification-feed'


async def run_application():
    app_logger = logging.getLogger(LOGGER_NAME)

    app_logger.debug('Examining environment...')
    port = os.environ['PORT']
    app_logger.debug('Examining environment: done')

    await create_incoming_application(port)


async def create_incoming_application(port):
    app_logger = logging.getLogger(LOGGER_NAME)

    async def handle(_):
        return web.json_response(get_page())

    app_logger.debug('Creating listening web application...')
    app = web.Application()
    app.add_routes([web.get('/', handle)])

    access_log_format = '%a %t "%r" %s %b "%{Referer}i" "%{User-Agent}i" %{X-Forwarded-For}i'
    runner = web.AppRunner(app, access_log_format=access_log_format)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    app_logger.debug('Creating listening web application: done')


def setup_logging():
    stdout_handler = logging.StreamHandler(sys.stdout)
    aiohttp_log = logging.getLogger('aiohttp.access')
    aiohttp_log.setLevel(logging.DEBUG)
    aiohttp_log.addHandler(stdout_handler)

    app_logger = logging.getLogger(LOGGER_NAME)
    app_logger.setLevel(logging.DEBUG)
    app_logger.addHandler(stdout_handler)


def get_page():
    return {
        '@context': [
            'https://www.w3.org/ns/ettystreams',
            {
                'dit': 'https://www.trade.gov.uk/ns/activitystreams/v1'
            }
        ],
        'orderedItems': [
            {
                'actor': {
                    'dit:activityStreamVerificationFeedOrganizationId': '1',
                    'type': [
                        'Organization',
                        'dit:activityStreamVerificationFeedOrganization'
                    ]
                },
                'dit:application': 'activityStreamVerificationFeed',
                'id': f'dit:activityStreamVerificationFeed:Verifier:{activity_id}:Create',
                'object': {
                    'id': f'dit:activityStreamVerificationFeed:Verifier:{activity_id}',
                    'type': [
                        'Document',
                        'dit:activityStreamVerificationFeed:Verifier'
                    ],
                    'url': f'https://activitystream.uktrade.io/activities/{activity_id}'
                },
                'published': datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                'type': 'Create'
            }
            for i in range(0, 1000)
            for activity_id in [str(i)]
        ],
        'type': 'Collection'
    }


def main():
    setup_logging()

    loop = asyncio.get_event_loop()
    loop.create_task(run_application())
    loop.run_forever()


if __name__ == '__main__':
    main()
