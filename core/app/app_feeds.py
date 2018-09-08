import asyncio
from base64 import (
    b64encode,
)
import re

import yarl

from .app_hawk import (
    get_hawk_header,
)
from .app_utils import sub_dict_lower


def parse_feed_config(feed_config):
    by_feed_type = {
        'activity_stream': ActivityStreamFeed,
        'zendesk': ZendeskFeed,
    }
    return by_feed_type[feed_config['TYPE']].parse_config(feed_config)


class ActivityStreamFeed:

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

    def auth_headers(self, url):
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
    def convert_to_bulk_es(cls, feed, index_names):
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
            for index_name in index_names
        ]


class ZendeskFeed:

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

    def auth_headers(self, _):
        auth = b64encode(f'{self.api_email}/token:{self.api_key}'.encode('utf-8')).decode('utf-8')
        return {
            'Authorization': f'Basic {auth}'
        }

    @classmethod
    def convert_to_bulk_es(cls, page, index_names):
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
            for index_name in index_names
        ]


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
