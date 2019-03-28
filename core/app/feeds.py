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
        self.unique_id = unique_id
        self.seed = seed
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

    @staticmethod
    def get_lock():
        return asyncio.Lock()

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
    async def convert_to_bulk_es(cls, _, feed, activity_index_names, object_index_names):
        return [
            {
                'action_and_metadata': {
                    'index': {
                        '_id': item['id'],
                        '_index': index_name,
                        '_type': '_doc',
                    }
                },
                'source': item
            }
            for item in feed['orderedItems']
            for index_name in activity_index_names
        ] + [
            {
                'action_and_metadata': {
                    'index': {
                        '_id': item['object']['id'],
                        '_index': index_name,
                        '_type': '_doc',
                    }
                },
                'source': item['object']
            }
            for item in feed['orderedItems']
            for index_name in object_index_names
        ]


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
        self.unique_id = unique_id
        self.seed = seed
        self.api_email = api_email
        self.api_key = api_key

    @staticmethod
    def get_lock():
        return asyncio.Lock()

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
    async def convert_to_bulk_es(cls, _, page, activity_index_names, object_index_names):
        def company_numbers(description):
            match = re.search(cls.company_number_regex, description)
            return [match[1]] if match else []

        return [
            {
                'action_and_metadata': _action_and_metadata(
                    index_name=index_name,
                    activity_id=activity_id),
                'source': _source(
                    activity_id=activity_id,
                    activity_type='Create',
                    object_id='dit:zendesk:Ticket:' + str(ticket['id']),
                    published_date=ticket['created_at'],
                    dit_application='zendesk',
                    object_type='dit:zendesk:Ticket',
                    actor=_company_actor(companies_house_number=company_number)),
            }
            for ticket in page['tickets']
            for company_number in company_numbers(ticket['description'])
            for activity_id in ['dit:zendesk:Ticket:' + str(ticket['id']) + ':Create']
            for index_name in activity_index_names
        ] + [
            {
                'action_and_metadata': {
                    'index': {
                        '_index': index_name,
                        '_type': '_doc',
                        '_id': activity_id,
                    },
                },
                'source': _source(
                    activity_id=activity_id,
                    activity_type='Create',
                    object_id='dit:zendesk:Ticket:' + str(ticket['id']),
                    published_date=ticket['created_at'],
                    dit_application='zendesk',
                    object_type='dit:zendesk:Ticket',
                    actor=_company_actor(companies_house_number=company_number)
                )['object']
            }
            for ticket in page['tickets']
            for company_number in company_numbers(ticket['description'])
            for activity_id in ['dit:zendesk:Ticket:' + str(ticket['id']) + ':Create']
            for index_name in object_index_names
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
        self.unique_id = unique_id
        self.seed = seed
        self.account_id = account_id
        self.api_key = api_key
        self.auth_url = auth_url
        self.event_url = event_url
        self.accesstoken = None
        self.whitelisted_folders = whitelisted_folders

    @staticmethod
    def get_lock():
        return asyncio.Lock()

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

    async def convert_to_bulk_es(self, context, page, activity_index_names, object_index_names):
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
                'action_and_metadata': _action_and_metadata(
                    index_name=index_name,
                    activity_id='dit:aventri:Event:' + str(event['eventid']) + ':Create'),
                'source': {
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
                        'published': now
                    },

                }
            }
            for page_event in page
            for index_name in activity_index_names
            for event in [await get_event(page_event['eventid'])]
            if self.should_include(context, event)
        ] + [
            {
                'action_and_metadata': {
                    'index': {
                        '_index': index_name,
                        '_type': '_doc',
                        '_id':  'dit:aventri:Event:' + str(event['eventid']) + ':Create',
                    },
                },
                'source': {
                    'type': 'Event',
                    'id': 'dit:aventri:Event:' + str(event['eventid']),
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
                    'published': now
                },
            }
            for page_event in page
            for index_name in object_index_names
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


def _action_and_metadata(
        index_name,
        activity_id):
    return {
        'index': {
            '_index': index_name,
            '_type': '_doc',
            '_id': activity_id,
        },
    }


def _source(
        activity_id,
        activity_type,
        object_id,
        published_date,
        dit_application,
        object_type,
        actor):
    return {
        'id': activity_id,
        'type': activity_type,
        'published': published_date,
        'dit:application': dit_application,
        'actor': actor,
        'object': {
            'type': [
                'Document',
                object_type,
            ],
            'id': object_id,
        },
    }


def _company_actor(companies_house_number):
    return {
        'type': [
            'Organization',
            'dit:company',
        ],
        'dit:companiesHouseNumber': companies_house_number,
    }
