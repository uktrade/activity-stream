import re

import aiohttp
import mohawk

from .app_utils import sub_dict_lower


class ElasticsearchBulkFeed:

    polling_page_interval = 0
    polling_seed_interval = 5

    @classmethod
    def parse_config(cls, config):
        return cls(**sub_dict_lower(config, ['SEED', 'ACCESS_KEY_ID', 'SECRET_ACCESS_KEY']))

    def __init__(self, seed, access_key_id, secret_access_key):
        self.seed = seed
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key

    @staticmethod
    def next_href(feed):
        return feed.get('next_url', None)

    def auth_headers(self, url):
        method = 'GET'
        return {
            'Authorization': mohawk.Sender({
                'id': self.access_key_id,
                'key': self.secret_access_key,
                'algorithm': 'sha256'
            }, url, method, content_type='', content='').request_header,
        }

    @staticmethod
    def convert_to_bulk_es(feed):
        return feed['items']


class ZendeskFeed:

    # The staging API is severely rate limited
    # This could be dynamic, but KISS
    polling_page_interval = 30
    polling_seed_interval = 60

    company_number_regex = r'Company number:\s*(\d+)'

    @classmethod
    def parse_config(cls, config):
        return cls(**sub_dict_lower(config, ['SEED', 'API_EMAIL', 'API_KEY']))

    def __init__(self, seed, api_email, api_key):
        self.seed = seed
        self.api_email = api_email
        self.api_key = api_key

    @staticmethod
    def next_href(feed):
        return feed['next_page']

    def auth_headers(self, _):
        return {
            'Authorization': aiohttp.helpers.BasicAuth(
                login=self.api_email + '/token',
                password=self.api_key,
            ).encode()
        }

    @classmethod
    def convert_to_bulk_es(cls, page):
        def company_numbers(description):
            match = re.search(cls.company_number_regex, description)
            return [match[1]] if match else []

        return [
            _activity(
                activity_id='dit:zendesk:Ticket:' + ticket['id'] + ':Create',
                activity_type='Create',
                object_id='dit:zendesk:Ticket:' + ticket['id'],
                published_date=ticket['created_at'],
                dit_application='zendesk',
                object_type='dit:zendesk:Ticket',
                actor=_company_actor(companies_house_number=company_number),
            )
            for ticket in page['tickets']
            for company_number in company_numbers(ticket['description'])
        ]


def _activity(
        activity_id,
        activity_type,
        object_id,
        published_date,
        dit_application,
        object_type,
        actor,
):
    return {
        'action_and_metadata': {
            'index': {
                '_index': 'activities',
                '_type': '_doc',
                '_id': activity_id,
            },
        },
        'source': {
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
