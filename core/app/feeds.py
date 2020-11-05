from abc import ABCMeta, abstractmethod
import asyncio
import csv
import datetime
import re
from io import StringIO
import aiohttp
import yarl

from .hawk import (
    get_hawk_header,
)
from .http import (
    http_make_request,
    http_stream_read_lines,
)
from .logger import (
    logged,
)
from .metrics import (
    metric_timer,
)
from .utils import (
    json_loads,
    sub_dict_lower,
)
from .utils import (
    async_enumerate,
    sleep,
)


def parse_feed_config(feed_config):
    by_feed_type = {
        'activity_stream': ActivityStreamFeed,
        'zendesk': ZendeskFeed,
        'maxemail': MaxemailFeed,
    }
    return by_feed_type[feed_config['TYPE']].parse_config(feed_config)


class Feed(metaclass=ABCMeta):
    """
    Abstract base class for all feeds with default functionality defined
    """
    down_grace_period = 60 * 2

    full_ingest_page_interval = 0.25
    updates_page_interval = 1
    exception_intervals = [1, 2, 4, 8, 16, 32, 64]

    @classmethod
    @abstractmethod
    def parse_config(cls, config):
        pass

    @staticmethod
    def next_href(feed):
        return feed.get('next', None)

    @abstractmethod
    async def auth_headers(self, context, url):
        pass

    @classmethod
    @abstractmethod
    async def get_activities(cls, context, feed):
        pass

    @classmethod
    async def pages(cls, context, feed, href, ingest_type):
        """
        async generator yielding 200 records at a time
        """
        logger = context.logger

        async def fetch_page(context, href, headers):
            """Fetch a single page of data from a feed, returning it as bytes

            If a non-200 response is returned, an exception is raised. However, if a
            429 is returned with a Retry-After header, the fetch is retried after this
            time, up to 10 attempts. After 10 failed attempts, an exception is
            raised.

            Raised exceptions are expected to cause the current ingest cycle to fail,
            but be re-attempted some time later.
            """
            num_attempts = 0
            max_attempts = 10

            while True:
                num_attempts += 1
                try:
                    result = await http_make_request(
                        context.session, context.metrics, 'GET', href, data=b'', headers=headers)
                    result.raise_for_status()
                    return result._body
                except aiohttp.ClientResponseError as client_error:
                    if (num_attempts >= max_attempts or client_error.status != 429 or
                            'Retry-After' not in client_error.headers):
                        raise
                    logger.debug(
                        'HTTP 429 received at attempt (%s). Will retry after (%s) seconds',
                        num_attempts,
                        client_error.headers['Retry-After'],
                    )
                    await sleep(context, int(client_error.headers['Retry-After']))

        async def gen_source_pages(href):
            updates_href = href
            while updates_href:
                # Lock so there is only 1 request per feed at any given time
                async with feed.lock:
                    with \
                            logged(logger.info, logger.warning, 'Polling page (%s)',
                                   [updates_href]), \
                            metric_timer(context.metrics['ingest_page_duration_seconds'],
                                         [feed.unique_id, ingest_type, 'pull']):
                        feed_contents = await fetch_page(
                            context, updates_href, await feed.auth_headers(context, updates_href),
                        )

                with logged(logger.debug, logger.warning, 'Parsing JSON', []):
                    feed_parsed = json_loads(feed_contents)

                with logged(logger.debug, logger.warning, 'Convert to activities', []):
                    activities = await feed.get_activities(context, feed_parsed)

                yield activities, updates_href
                updates_href = feed.next_href(feed_parsed)

        async def gen_evenly_sized_pages(source_pages):
            # pylint: disable=undefined-loop-variable
            page_size = 200
            current = []
            async for activities, updates_href in source_pages:
                current.extend(activities)

                while len(current) >= page_size:
                    to_yield, current = current[:page_size], current[page_size:]
                    yield to_yield, updates_href

            if current:
                yield current, updates_href

        source_pages = gen_source_pages(href)
        evenly_sized_pages = gen_evenly_sized_pages(source_pages)

        # Would be nicer if could "yield from", but there is no
        # language support for doing that in async generator
        async for page, updates_href in evenly_sized_pages:
            yield page, updates_href


class ActivityStreamFeed(Feed):

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

    @classmethod
    async def get_activities(cls, _, feed):
        return feed['orderedItems']

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


class ZendeskFeed(Feed):
    down_grace_period = 400

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
    async def get_activities(cls, _, feed):
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
            for ticket in feed['tickets']
            for company_number in company_numbers(ticket['description'])
        ]


class MaxemailFeed(Feed):

    down_grace_period = 60 * 60 * 4

    full_ingest_page_interval = 3
    updates_page_interval = 60 * 60 * 24 * 30
    exception_intervals = [120, 180, 240, 300]

    @classmethod
    def parse_config(cls, config):
        return cls(**sub_dict_lower(
            config,
            [
                'UNIQUE_ID',
                'SEED',
                'DATA_EXPORT_URL',
                'CAMPAIGN_URL',
                'USERNAME',
                'PASSWORD',
                'PAGE_SIZE'
            ]
        ))

    def __init__(self, unique_id, seed, data_export_url,
                 campaign_url, username, password, page_size):
        self.lock = asyncio.Lock()
        self.unique_id = unique_id
        self.seed = seed
        self.data_export_url = data_export_url
        self.campaign_url = campaign_url
        self.username = username
        self.password = password
        self.page_size = int(page_size)

    @staticmethod
    def next_href(_):
        """
        Maxemail API does not support GET requests with next href for pagination
        returns (None)
        """
        return None

    async def auth_headers(self, _, __):
        return {
            'Authorization': aiohttp.helpers.BasicAuth(
                login=self.username,
                password=self.password,
            ).encode()
        }

    async def get_activities(self, context, feed):
        return None

    @classmethod
    async def pages(cls, context, feed, href, ingest_type):
        """
        async generator for Maxemail email campaign sent records
        """
        # pylint: disable=too-many-statements
        timestamp = href
        logger = context.logger
        campaigns = {}

        async def get_email_campaign(email_campaign_id):
            if email_campaign_id not in campaigns:
                campaigns[email_campaign_id] = await fetch_email_campaign_from_maxemail(
                    email_campaign_id
                )
            return campaigns[email_campaign_id]

        async def fetch_email_campaign_from_maxemail(email_campaign_id):
            """
            Retrieves email campaign data for the given id
            url: root/api/json/email_campaign
            method': 'find'
            param: 'emailId'
            """
            url = feed.campaign_url
            payload = {'method': 'find', 'emailId': email_campaign_id}

            with logged(context.logger.debug, context.logger.warning,
                        'maxemail Fetching campaign (%s) with payload (%s)', [url, payload]):
                result = await http_make_request(
                    context.session,
                    context.metrics,
                    'POST',
                    url,
                    data=payload,
                    headers=await feed.auth_headers(None, None),
                )
                result.raise_for_status()

            campaign = json_loads(result._body)
            campaign_name = campaign.get('name')

            return campaign_name

        async def get_data_export_key(timestamp, method):
            """
            url: root/api/json/data_export_quick

            sample payload {'method': 'sent', 'filter': '{"timestamp": "2020-09-10 17:00:00"}'}
            """
            payload_filter = '{"timestamp": "' + timestamp + '"}'
            payload = {
                'method': method,
                'filter': payload_filter
            }

            url = feed.seed
            num_attempts = 0
            max_attempts = 10

            while True:
                num_attempts += 1
                try:
                    with logged(context.logger.info, context.logger.warning,
                                'maxemail export key (%s) with payload (%s)', [url, payload]):
                        result = await http_make_request(
                            context.session,
                            context.metrics,
                            'POST',
                            url,
                            data=payload,
                            headers=await feed.auth_headers(None, None),
                        )
                        key = str(await result.text()).strip('\"')
                        return key
                except aiohttp.ClientResponseError as client_error:
                    if (num_attempts >= max_attempts or client_error.status != 429 or
                            'Retry-After' not in client_error.headers):
                        raise
                    logger.debug(
                        'HTTP 429 received at attempt (%s). Will retry after (%s) seconds',
                        num_attempts,
                        client_error.headers['Retry-After'],
                    )
                    await sleep(context, int(client_error.headers['Retry-After']))

        async def gen_data_export_csv(key):
            """
            url: root/file/key/{key}
            """
            url = feed.data_export_url.format(key=key)
            with logged(context.logger.debug, context.logger.warning,
                        'maxemail data export csv (%s)', [url]):
                lines = http_stream_read_lines(
                    context.session,
                    context.metrics,
                    'POST',
                    url,
                    data={},
                    headers=await feed.auth_headers(None, None),
                )

            headers = []
            async for i, line in async_enumerate(lines):
                try:
                    parsed_line = next(csv.reader(
                        StringIO(line.decode('utf-8')), skipinitialspace=True, delimiter=',',
                        quotechar='"', quoting=csv.QUOTE_ALL,
                    ))
                except StopIteration:
                    break

                if i == 0:
                    headers = parsed_line
                else:
                    yield dict(zip(headers, parsed_line))

        async def common(campaign_id, timestamp, email_address):
            campaign_name = await get_email_campaign(campaign_id)
            year, time = timestamp.split(' ')
            timestamp = f'{year}T{time}'
            line_id = f'{campaign_id}:{timestamp}:{email_address}'
            campaign_dict = {
                'type': 'dit:maxemail:Campaign',
                'id': 'dit:maxemail:Campaign:' + campaign_id,
                'name': campaign_name,
            }
            return campaign_dict, line_id, timestamp

        async def gen_sent_activities_and_last_updated(csv_lines):
            async for line in csv_lines:
                campaign_dict, line_id, timestamp = \
                    await common(line['email id'], line['sent timestamp'], line['email address'])
                activity = {
                    'id': 'dit:maxemail:Email:Sent:' + line_id + ':Create',
                    'type': 'Create',
                    'dit:application': 'maxemail',
                    'published': timestamp,
                    'object': {
                        'type': ['dit:maxemail:Email', 'dit:maxemail:Email:Sent'],
                        'id': 'dit:maxemail:Email:Sent:' + line_id,
                        'dit:emailAddress': line['email address'],
                        'attributedTo': campaign_dict
                    }
                }
                yield activity, timestamp

        async def paginate(page_size, objs):
            page = []
            last_updated = None
            async for obj, last_updated in objs:
                page.append(obj)
                if len(page) == page_size:
                    yield page, last_updated
                    page = []

            if page:
                yield page, last_updated

        now = datetime.datetime.now()
        if ingest_type == 'full':
            # get last 6 weeks for full ingestion
            six_weeks_ago = now - datetime.timedelta(days=42)
            timestamp = six_weeks_ago.strftime('%Y-%m-%d 00:00:00')

        sent_data_export_key = await get_data_export_key(timestamp, 'sent')
        sent_csv_lines = gen_data_export_csv(sent_data_export_key)
        sent_activities_and_last_updated = gen_sent_activities_and_last_updated(sent_csv_lines)
        activity_pages_and_last_updated = paginate(
            feed.page_size, sent_activities_and_last_updated)

        async for activity_page, last_updated in activity_pages_and_last_updated:
            yield activity_page, last_updated
