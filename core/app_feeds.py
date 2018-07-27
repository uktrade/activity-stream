import re

import aiohttp
import mohawk

from .app_utils import sub_dict_lower


class ActivityStreamFeed:

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
        return feed.get('next', None)

    def auth_headers(self, url):
        method = 'GET'
        return {
            'Authorization': mohawk.Sender({
                'id': self.access_key_id,
                'key': self.secret_access_key,
                'algorithm': 'sha256'
            }, url, method, content_type='', content='').request_header,
        }

    @classmethod
    def convert_to_bulk_es(cls, feed, index_name):
        return [{
            'action_and_metadata': {
                'index': {
                    '_id': item['id'],
                    '_index': index_name,
                    '_type': '_doc',
                }
            },
            'source': item
        } for item in feed['orderedItems']]


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
    def convert_to_bulk_es(cls, page, index_name):
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
