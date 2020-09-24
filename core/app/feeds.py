from abc import ABCMeta, abstractmethod
import asyncio
import csv
import codecs
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
    json_dumps,
    sub_dict_lower,
)
from .utils import (
    sleep,
)


def parse_feed_config(feed_config):
    by_feed_type = {
        'activity_stream': ActivityStreamFeed,
        'zendesk': ZendeskFeed,
        'aventri': EventFeed,
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


class EventFeed(Feed):

    down_grace_period = 60 * 60 * 4

    full_ingest_page_interval = 3
    updates_page_interval = 60 * 60 * 24 * 30
    exception_intervals = [120, 180, 240, 300]

    @classmethod
    def parse_config(cls, config):
        return cls(**sub_dict_lower(
            config, ['UNIQUE_ID', 'SEED', 'ACCOUNT_ID', 'API_KEY', 'AUTH_URL', 'EVENT_URL',
                     'WHITELISTED_FOLDERS', 'GETADDRESS_API_KEY', 'GETADDRESS_API_URL'
                     ]))

    def __init__(self, unique_id, seed, account_id, api_key,
                 auth_url, event_url, whitelisted_folders, getaddress_api_key, getaddress_api_url):
        self.lock = asyncio.Lock()
        self.unique_id = unique_id
        self.seed = seed
        self.account_id = account_id
        self.api_key = api_key
        self.auth_url = auth_url
        self.event_url = event_url
        self.getaddress_api_key = getaddress_api_key
        self.getaddress_api_url = getaddress_api_url
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

        self.accesstoken = json_loads(result._body)['accesstoken']
        return {
            'accesstoken': self.accesstoken,
        }

    async def get_activities(self, context, feed):
        async def get_event(event_id):
            event_lookup = await context.redis_client.execute('GET', f'event-{event_id}')
            if event_lookup:
                try:
                    return json_loads(event_lookup.decode('utf-8'))
                except UnicodeDecodeError:
                    await context.redis_client.execute('DEL', f'event-{event_id}')
                    return await fetch_from_aventri(event_id)
            else:
                return await fetch_from_aventri(event_id)

        def can_get_location(event):
            return event.get('location') and event['location'].get('postcode')

        async def fetch_from_aventri(event_id):
            url = self.event_url.format(event_id=event_id)

            with logged(context.logger.debug, context.logger.warning,
                        'Fetching event (%s)', [url]):
                result = await http_make_request(
                    context.session, context.metrics, 'GET', url, data=b'',
                    headers={'accesstoken': self.accesstoken})
                result.raise_for_status()

            event = json_loads(result._body)
            if can_get_location(event):
                event = await get_location(event)

            await context.redis_client.execute(
                'SETEX', f'event-{event_id}', 60*60*24*7, json_dumps(event))
            return event

        async def get_location(event):
            postcode = event['location']['postcode']

            # Search Reddis first
            redis_lookup = await context.redis_client.execute('GET', f'address-{postcode}')
            if redis_lookup:
                split_lat_lng = redis_lookup.decode('utf-8').split(',')
                event['geocoordinates'] = {}
                event['geocoordinates']['lat'] = split_lat_lng[0]
                event['geocoordinates']['lon'] = split_lat_lng[1]
            else:  # Try AddressLookup API
                event = await fetch_address_from_getaddressio(event, postcode)
            return event

        async def fetch_address_from_getaddressio(event, postcode):
            url = self.getaddress_api_url + \
                f'/find/{postcode}?api-key={self.getaddress_api_key}'
            resp = await http_make_request(context.session, context.metrics, 'GET', url,
                                           data=b'', headers={})
            if resp.status == 200:
                geo_result = json_loads(await resp.text())
                if geo_result.get('latitude'):
                    event['geocoordinates'] = {}
                    event['geocoordinates']['lat'] = str(
                        geo_result['latitude'])
                    event['geocoordinates']['lon'] = str(
                        geo_result['longitude'])
                    joined_lat_lng = ','.join(
                        [str(geo_result['latitude']),
                         str(geo_result['longitude'])]
                    ).encode('utf-8')
                    await context.redis_client.execute(
                        'SET', f'address-{postcode}', joined_lat_lng)
            return event

        now = datetime.datetime.now().isoformat()
        return [
            {
                'id': 'dit:aventri:Event:' + str(event['eventid']) + ':Create',
                'type': 'Search',
                'eventid': event['eventid'],
                'dit:application': 'aventri',
                'object': {
                    'type': ['Event', 'dit:aventri:Event'],
                    'id': 'dit:aventri:Event:' + event['eventid'],
                    'name': event['name'],
                    'url': event['url'],
                    'content': event['description'],
                    'startdate': event['startdate'],
                    'enddate': event['enddate'],
                    'foldername': event['foldername'],
                    'location': event['location'],
                    'geocoordinates': event.get('geocoordinates'),
                    'language': event['defaultlanguage'],
                    'timezone': event['timezone'],
                    'currency': event['standardcurrency'],
                    'price_type': event['price_type'],
                    'price': event['pricepoints'],
                    'published': now,
                }
            }
            for page_event in feed
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

        async def get_data_export_key(timestamp):
            """
            url: root/api/json/data_export_quick
            method: 'sent'

            sample payload {'method': 'sent', 'filter': '{"timestamp": "2020-09-10 17:00:00"}'}
            """
            payload_filter = '{"timestamp": "' + timestamp + '"}'
            payload = {
                'method': 'sent',
                'filter': f'{payload_filter}'
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
            row_count = 0
            async for line in lines:
                # skip header
                if row_count == 0:
                    row_count += 1
                    continue
                yield codecs.decode(line, 'utf-8')

        async def gen_parse_rows_for_bulk_insert(data_export_key):
            parsed = []
            async for line in gen_data_export_csv(data_export_key):
                try:
                    parsed_line = next(csv.reader(
                        StringIO(line), skipinitialspace=True, delimiter=',',
                        quotechar="'", quoting=csv.QUOTE_ALL,
                    ))
                except StopIteration:
                    break
                else:
                    campaign_name = await get_email_campaign(parsed_line[0])
                    # email_campaign_id-email_address
                    line_id = f'{parsed_line[0]}-{parsed_line[1]}'
                    last_updated = parsed_line[2]
                    parsed.append(
                        {
                            'id': 'dit:maxemail:Email:' + line_id + ':Create',
                            'type': 'Create',
                            'dit:application': 'maxemail',
                            'object': {
                                'type': ['Document', 'dit:maxemail:Email'],
                                'id': 'dit:maxemail:Email:' + line_id,
                                'dit:emailAddress': parsed_line[1],
                                'attributedTo': {
                                    'type': 'dit:maxemail:Campaign',
                                    'id': 'dit:maxemail:Campaign:id' + parsed_line[0],
                                    'name': campaign_name,
                                    'published': parsed_line[2],
                                }
                            }
                        }
                    )
                    if len(parsed) == feed.page_size:
                        yield parsed, last_updated
                        parsed = []

            if parsed:
                yield parsed, last_updated

        now = datetime.datetime.now()
        if ingest_type == 'full':
            # get last 6 weeks for full ingestion
            six_weeks_ago = now - datetime.timedelta(days=42)
            timestamp = six_weeks_ago.strftime('%Y-%m-%d 00:00:00')

        data_export_key = await get_data_export_key(timestamp)
        logger.debug('maxemail export key (%s)', data_export_key)

        async for rows, last_updated in gen_parse_rows_for_bulk_insert(data_export_key):
            yield rows, last_updated
