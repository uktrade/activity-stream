import asyncio
import datetime
import json
import re
import aiohttp
import yarl

from .hawk import (
    get_hawk_header,
)
from .http import (
    http_make_request,
)
from .logger import (
    logged,
)
from .utils import sub_dict_lower


def parse_feed_config(feed_config):
    by_feed_type = {
        'activity_stream': ActivityStreamFeed,
        'zendesk': ZendeskFeed,
        'aventri': EventFeed,
    }
    return by_feed_type[feed_config['TYPE']].parse_config(feed_config)


class ActivityStreamFeed:

    max_interval_before_reporting_down = 60

    full_ingest_page_interval = 0.25
    updates_page_interval = 1
    exception_intervals = [1, 2, 4, 8, 16, 32, 64]

    @classmethod
    def parse_config(cls, config):
        return cls(**sub_dict_lower(config,
                                    ['UNIQUE_ID', 'SEED', 'ACCESS_KEY_ID', 'SECRET_ACCESS_KEY']))

    def __init__(self, unique_id, seed, access_key_id, secret_access_key):
        self.lock = asyncio.Lock()
        self.unique_id = unique_id
        self.seed = seed
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

    @staticmethod
    def next_href(feed):
        return feed.get('next', None)

    async def auth_headers(self, _, url):
        parsed_url = yarl.URL(url)
        return {
            'Authorization': get_hawk_header(
                access_key_id=self.access_key_id,
                secret_access_key=self.secret_access_key,
                method='GET',
                host=parsed_url.host,
                port=str(parsed_url.port),
                path=parsed_url.raw_path_qs,
                content_type=b'',
                content=b'',
            )
        }

    @classmethod
    async def get_activities(cls, _, feed):
        return feed['orderedItems']


class ZendeskFeed:

    max_interval_before_reporting_down = 400

    # The staging API is severely rate limited
    # Could be higher on prod, but KISS
    full_ingest_page_interval = 30
    updates_page_interval = 120
    exception_intervals = [120, 180, 240, 300]

    company_number_regex = r'Company number:\s*(\d+)'

    @classmethod
    def parse_config(cls, config):
        return cls(**sub_dict_lower(config, ['UNIQUE_ID', 'SEED', 'API_EMAIL', 'API_KEY']))

    def __init__(self, unique_id, seed, api_email, api_key):
        self.lock = asyncio.Lock()
        self.unique_id = unique_id
        self.seed = seed
        self.api_email = api_email
        self.api_key = api_key

    @staticmethod
    def next_href(feed):
        return feed['next_page']

    async def auth_headers(self, _, __):
        return {
            'Authorization': aiohttp.helpers.BasicAuth(
                login=self.api_email + '/token',
                password=self.api_key,
            ).encode()
        }

    @classmethod
    async def get_activities(cls, _, page):
        def company_numbers(description):
            match = re.search(cls.company_number_regex, description)
            return [match[1]] if match else []

        return [
            {
                'id': 'dit:zendesk:Ticket:' + str(ticket['id']) + ':Create',
                'type': 'Create',
                'published': ticket['created_at'],
                'dit:application': 'zendesk',
                'actor': {
                    'type': [
                        'Organization',
                        'dit:company',
                    ],
                    'dit:companiesHouseNumber': company_number,
                },
                'object': {
                    'type': [
                        'Document',
                        'dit:zendesk:Ticket',
                    ],
                    'id': 'dit:zendesk:Ticket:' + str(ticket['id']),
                },
            }
            for ticket in page['tickets']
            for company_number in company_numbers(ticket['description'])
        ]


class EventFeed:

    max_interval_before_reporting_down = 60 * 60 * 4

    full_ingest_page_interval = 3
    updates_page_interval = 60 * 60 * 24 * 30
    exception_intervals = [120, 180, 240, 300]

    @classmethod
    def parse_config(cls, config):
        return cls(**sub_dict_lower(
            config, ['UNIQUE_ID', 'SEED', 'ACCOUNT_ID', 'API_KEY', 'AUTH_URL', 'EVENT_URL',
                     'WHITELISTED_FOLDERS']))

    def __init__(self, unique_id, seed, account_id, api_key, auth_url, event_url,
                 whitelisted_folders):
        self.lock = asyncio.Lock()
        self.unique_id = unique_id
        self.seed = seed
        self.account_id = account_id
        self.api_key = api_key
        self.auth_url = auth_url
        self.event_url = event_url
        self.accesstoken = None
        self.whitelisted_folders = whitelisted_folders

    @staticmethod
    def next_href(_):
        """ aventri API does not support pagination
            returns (None)
        """
        return None

    async def auth_headers(self, context, __):
        result = await http_make_request(
            context.session, context.metrics, 'POST', self.auth_url, data={
                'accountid': self.account_id, 'key': self.api_key,
            }, headers={})
        result.raise_for_status()

        self.accesstoken = json.loads(result._body.decode('utf-8'))['accesstoken']
        return {
            'accesstoken': self.accesstoken,
        }

    async def get_activities(self, context, page):
        async def get_event(event_id):
            await asyncio.sleep(3)
            url = self.event_url.format(event_id=event_id)

            with logged(context.logger, 'Fetching event (%s)', [url]):
                result = await http_make_request(
                    context.session, context.metrics, 'GET', url, data=b'',
                    headers={'accesstoken': self.accesstoken})
                result.raise_for_status()

            return json.loads(result._body.decode('utf-8'))

        now = datetime.datetime.now().isoformat()
        return [
            {
                'id': 'dit:aventri:Event:' + str(event['eventid']) + ':Create',
                'type': 'Search',
                'eventid': event['eventid'],
                'dit:application': 'aventri',
                'object': {
                    'type': [
                        'Document',
                        'dit:aventri:Event',
                    ],
                    'id': 'dit:aventri:Event:' + event['eventid'],
                    'name': event['name'],
                    'url': event['url'],
                    'content': event['description'],
                    'startdate': event['startdate'],
                    'enddate': event['enddate'],
                    'foldername': event['foldername'],
                    'location': event['location'],
                    'language': event['defaultlanguage'],
                    'timezone': event['timezone'],
                    'currency': event['standardcurrency'],
                    'price_type': event['price_type'],
                    'price': event['pricepoints'],
                    'published': now,
                }
            }
            for page_event in page
            for event in [await get_event(page_event['eventid'])]
            if self.should_include(context, event)
        ]

    def should_include(self, context, event):
        # event must be not deleted
        # startdate should be >= today and not null
        # enddate should be >= startdate and not null
        # folderid or foldername should be != internal events folder
        # name, url, description should be not null

        allowed_folders = self.whitelisted_folders.split(',')
        now = datetime.datetime.today().strftime('%Y-%m-%d')
        try:
            should_include = (
                event['eventid'] is not None and
                event['deleted'] != 0 and
                event['enddate'] >= event['startdate'] >= now and
                event['name'] is not None and
                event['url'] is not None and
                event['description'] is not None and
                event['include_calendar'] == '1' and
                event['status'] == 'Live' and
                event['foldername'] in allowed_folders
            )

        except KeyError:
            should_include = False

        loggable_event = {
            key: event[key]
            for key in (
                'eventid', 'deleted', 'enddate', 'startdate', 'name', 'url', 'description',
                'include_calendar', 'status', 'foldername',
            )
            if key in event
        }
        context.logger.debug('Event data: (%s) should_include: (%s)',
                             loggable_event, should_include)

        return should_include
