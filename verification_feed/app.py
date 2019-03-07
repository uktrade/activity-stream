import asyncio
from datetime import (
    datetime,
    timedelta,
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
    port = os.environ['VERIFICATION_FEED_PORT']
    app_logger.debug('Examining environment: done')

    await create_incoming_application(port)


async def create_incoming_application(port):
    app_logger = logging.getLogger(LOGGER_NAME)

    async def handle(request):
        timestamp = int(request.match_info['timestamp'])

        def get_next_page_href(next_timestamp):
            return str(request.url.with_scheme(request.headers.get(
                'X-Forwarded-Proto', 'http')).with_path(f'/{next_timestamp}'))
        return web.json_response(get_page(timestamp, get_next_page_href))

    app_logger.debug('Creating listening web application...')
    app = web.Application()
    app.add_routes([web.get(r'/{timestamp:\d+}', handle)])

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


def get_page(timestamp, get_next_page_href):
    ''' Creates dummy activities where one has been created every second for the past 24 hours'''
    now = datetime.now(timezone.utc).replace(microsecond=0)
    one_day_ago = now - timedelta(minutes=5)

    first_timestamp = int(one_day_ago.timestamp())
    final_timestamp = int(now.timestamp())

    max_per_page = 1000
    first_timestamp_of_page = max(first_timestamp, timestamp)
    final_timestamp_of_page = min(first_timestamp_of_page + max_per_page, final_timestamp)
    timestamps = range(first_timestamp_of_page, final_timestamp_of_page)

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
                'published': datetime.utcfromtimestamp(timestamp).isoformat(),
                'type': 'Create'
            }
            for timestamp in timestamps
            for activity_id in [str(timestamp)]
        ],
        'type': 'Collection',
        **({'next': get_next_page_href(final_timestamp_of_page)} if timestamps else {}),
    }


def main():
    setup_logging()

    loop = asyncio.get_event_loop()
    loop.create_task(run_application())
    loop.run_forever()


if __name__ == '__main__':
    main()
