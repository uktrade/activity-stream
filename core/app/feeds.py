from abc import ABCMeta, abstractmethod
import asyncio
from collections.abc import Sequence
import csv
import datetime
from distutils.util import strtobool
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
    json_dumps,
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
        'aventri': EventFeed,
    }
    return by_feed_type[feed_config['TYPE']].parse_config(feed_config)


class Feed(metaclass=ABCMeta):
    """
    Abstract base class for all feeds with default functionality defined
    """
    down_grace_period = 60 * 2

    full_ingest_page_interval = 0.25
    full_ingest_interval = 120
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
    full_ingest_page_interval = 1
    full_ingest_interval = 60 * 60

    updates_page_interval = 60 * 60 * 24 * 30
    exception_intervals = [120, 180, 240, 300]

    # This is quite small so even when we have a lot of sleeps, we still have signs that the
    # feed is working in Grafana
    ingest_page_size = 20

    @classmethod
    def parse_config(cls, config):
        return cls(**sub_dict_lower(
            config, ['UNIQUE_ID', 'SEED', 'ACCOUNT_ID', 'API_KEY', 'AUTH_URL', 'EVENT_URL',
                     'ATTENDEES_LIST_URL', 'ATTENDEE_URL'
                     ]))

    def __init__(self, unique_id, seed, account_id, api_key, auth_url,
                 event_url, attendees_list_url, attendee_url):
        self.lock = asyncio.Lock()
        self.unique_id = unique_id
        self.seed = seed
        self.account_id = account_id
        self.api_key = api_key
        self.auth_url = auth_url
        self.event_url = event_url
        self.attendees_list_url = attendees_list_url
        self.attendee_url = attendee_url
        self.accesstoken = None

    @staticmethod
    def next_href(_):
        """ aventri API does not support pagination
            returns (None)
        """
        return None

    async def auth_headers(self, _, __):
        return {}

    async def http_make_aventri_request(self, context, method, url, data, headers, sleep_interval):
        logger = context.logger

        num_attempts = 0
        max_attempts = 10
        retry_interval = 65

        while True:
            num_attempts += 1
            retry_interval += 60
            try:
                result = await http_make_request(
                    context.session, context.metrics, method, url, data=data, headers=headers)
                result.raise_for_status()
                if sleep_interval:
                    await sleep(context, sleep_interval)
                return result
            except aiohttp.ClientResponseError as client_error:
                if (num_attempts >= max_attempts or client_error.status not in [429, 502]):
                    raise
                logger.debug(
                    'HTTP %s received at attempt (%s). Will retry after (%s) seconds',
                    client_error.status,
                    num_attempts,
                    retry_interval,
                )
                context.raven_client.captureMessage(
                    f'HTTP {client_error.status} received at attempt ({num_attempts}).'
                    f'Will retry after ({retry_interval}) seconds',
                )
                await sleep(context, retry_interval)

    async def pages(self, context, feed, href, ingest_type):
        logger = context.logger

        async def gen_source_pages(href):
            # Lock so there is only 1 request per feed at any given time
            async with feed.lock:

                # Fetch access token for later requests
                result = await http_make_request(
                    context.session, context.metrics, 'POST', self.auth_url, data={
                        'accountid': self.account_id, 'key': self.api_key,
                    }, headers={}
                )
                result.raise_for_status()
                headers = {
                    'accesstoken': json_loads(result._body)['accesstoken'],
                }

                maybe_remaining = True
                total_events = 0
                per_page = 2000  # This is the max Aventri seems to allow

                while maybe_remaining:
                    with \
                            logged(logger.info, logger.warning, 'Polling page (%s)',
                                   [href]), \
                            metric_timer(context.metrics['ingest_page_duration_seconds'],
                                         [feed.unique_id, ingest_type, 'pull']):

                        result = await self.http_make_aventri_request(
                            context, 'GET', href, data=json_dumps({
                                'limit': per_page,
                                'offset': total_events,
                            }), headers=headers, sleep_interval=2
                        )
                        page_of_events = json_loads(result._body)
                        num_events_in_page = len(page_of_events)
                        total_events += num_events_in_page

                    with logged(logger.debug, logger.warning, 'Convert to activities', []):
                        activities = await feed.get_activities(context, page_of_events, headers)

                    async for activity in activities:
                        yield activity, href

                    maybe_remaining = num_events_in_page == per_page

        async def gen_evenly_sized_pages(source_pages):
            # pylint: disable=undefined-loop-variable
            page_size = self.ingest_page_size
            current = []
            async for activities, updates_href in source_pages:
                current.append(activities)

                while len(current) >= page_size:
                    to_yield, current = current[:page_size], current[page_size:]
                    yield to_yield, updates_href

            if current:
                yield current, updates_href

        source_pages = gen_source_pages(href)
        evenly_sized_pages = gen_evenly_sized_pages(source_pages)

        async for page, updates_href in evenly_sized_pages:
            yield page, updates_href

    async def get_activities(self, context, page_of_events, headers):
        # pylint: disable=bad-continuation
        async def get_attendee(event_id, attendee_id):
            attendee_lookup = await context.redis_client.execute(
                'GET', f'event-{event_id}-attendee-{attendee_id}'
            )
            if attendee_lookup:
                try:
                    return json_loads(attendee_lookup.decode('utf-8'))
                except UnicodeDecodeError:
                    await context.redis_client.execute(
                        'DEL', f'event-{event_id}-attendee-{attendee_id}'
                    )
                    return await fetch_attendee(event_id, attendee_id)
            else:
                return await fetch_attendee(event_id, attendee_id)

        async def fetch_attendee(event_id, attendee_id):
            url = self.attendee_url.format(event_id=event_id, attendee_id=attendee_id)
            with logged(context.logger.debug, context.logger.warning,
                        'Fetching attendee (%s)', [url]):
                result = await self.http_make_aventri_request(
                    context, 'GET', url, data=b'',
                    headers=headers, sleep_interval=2)
            attendee = json_loads(result._body)

            if 'error' in attendee:
                return None

            await context.redis_client.execute(
                'SETEX', f'event-{event_id}-attendee-{attendee_id}',
                60*60*24*2, json_dumps(attendee)
            )

            return attendee

        async def get_attendees(event_id):
            url = self.attendees_list_url.format(event_id=event_id)

            with logged(context.logger.debug, context.logger.warning,
                        'Fetching attendee list (%s)', [url]):
                result = await self.http_make_aventri_request(
                    context, 'GET', url, data=b'',
                    headers=headers, sleep_interval=2)
            attendees_list = json_loads(result._body)

            if 'error' in attendees_list:
                return []

            return [await get_attendee(event_id, a['attendeeid']) for a in attendees_list]

        async def get_event(event_id):
            event_lookup = await context.redis_client.execute('GET', f'event-{event_id}')
            if event_lookup:
                try:
                    return json_loads(event_lookup.decode('utf-8'))
                except UnicodeDecodeError:
                    await context.redis_client.execute('DEL', f'event-{event_id}')
                    return await fetch_event(event_id)
            else:
                return await fetch_event(event_id)

        async def fetch_event(event_id):
            url = self.event_url.format(event_id=event_id)

            with logged(context.logger.debug, context.logger.warning,
                        'Fetching event (%s)', [url]):
                result = await self.http_make_aventri_request(
                    context, 'GET', url, data=b'',
                    headers=headers, sleep_interval=2)
            event = json_loads(result._body)

            if 'error' in event or 'eventid' not in event:
                return None

            await context.redis_client.execute(
                'SETEX', f'event-{event_id}', 60*60*24*2, json_dumps(event))

            return event

        def map_to_activity(event_id, aventri_object):
            now = datetime.datetime.now().isoformat()
            if 'eventid' in aventri_object:
                return {
                    'id': 'dit:aventri:Event:' + event_id + ':Create',
                    'published': now,
                    'type': 'dit:aventri:Event',
                    'dit:application': 'aventri',
                    'object': {
                        'id': 'dit:aventri:Event:' + event_id,
                        'name': aventri_object['name'],
                        'published': datetime.datetime.strptime(
                            aventri_object['createddatetime'], '%Y-%m-%d %H:%M:%S'
                        ).isoformat(),

                        # The following mappings are used to allow great.gov.uk
                        # search to filter on events.
                        'attributedTo': {
                            'type': 'dit:aventri:Folder',
                            'id': f'dit:aventri:Folder:{aventri_object["foldername"]}'
                        },
                        'content': aventri_object['description'],
                        'dit:public': bool(strtobool(aventri_object['include_calendar'])),
                        'dit:status': aventri_object['status'],
                        'endTime': aventri_object['enddate'] + (
                            'T' + aventri_object['endtime'] if aventri_object['endtime'] else ''
                        ),
                        'startTime': aventri_object['startdate'] + (
                            'T' + \
                            aventri_object['starttime'] if aventri_object['starttime'] else ''
                        ),
                        'type': ['dit:aventri:Event'] + (
                            ['Tombstone'] if aventri_object['deleted'] == '1' else []
                        ),
                        'url': aventri_object['url'],

                        'dit:aventri:approval_required': aventri_object['approval_required'],
                        'dit:aventri:approval_status': aventri_object['approval_status'],
                        'dit:aventri:city': aventri_object['city'],
                        'dit:aventri:clientcontact': aventri_object['clientcontact'],
                        'dit:aventri:closedate': aventri_object['closedate'],
                        'dit:aventri:closetime': aventri_object['closetime'],
                        'dit:aventri:code': aventri_object['code'],
                        'dit:aventri:contactinfo': aventri_object['contactinfo'],
                        'dit:aventri:country': aventri_object['country'],
                        'dit:aventri:createdby': aventri_object['createdby'],
                        'dit:aventri:defaultlanguage': aventri_object['defaultlanguage'],
                        'dit:aventri:folderid': aventri_object['folderid'],
                        'dit:aventri:live_date': aventri_object['live_date'],
                        'dit:aventri:location_address1': aventri_object['location']['address1']
                        if aventri_object['location'] else None,
                        'dit:aventri:location_address2': aventri_object['location']['address2']
                        if aventri_object['location'] else None,
                        'dit:aventri:location_address3': aventri_object['location']['address3']
                        if aventri_object['location'] else None,
                        'dit:aventri:location_city': aventri_object['location']['city']
                        if aventri_object['location'] else None,
                        'dit:aventri:location_country': aventri_object['location']['country']
                        if aventri_object['location'] else None,
                        'dit:aventri:location_name': aventri_object['location']['name']
                        if aventri_object['location'] else None,
                        'dit:aventri:location_postcode': aventri_object['location']['postcode']
                        if aventri_object['location'] else None,
                        'dit:aventri:location_state': aventri_object['location']['state']
                        if aventri_object['location'] else None,
                        'dit:aventri:locationname': aventri_object['locationname'],
                        'dit:aventri:login1': aventri_object['login1'],
                        'dit:aventri:login2': aventri_object['login2'],
                        'dit:aventri:max_reg': aventri_object['max_reg'],
                        'dit:aventri:modifiedby': aventri_object['modifiedby'],
                        'dit:aventri:modifieddatetime': aventri_object['modifieddatetime'],
                        'dit:aventri:price_type': aventri_object['price_type'],
                        'dit:aventri:pricepoints': aventri_object['pricepoints'],
                        'dit:aventri:standardcurrency': aventri_object['standardcurrency'],
                        'dit:aventri:state': aventri_object['state'],
                        'dit:aventri:timezone': aventri_object['timezone'],
                    }
                }
            if not aventri_object['responses']:
                responses = []
            elif isinstance(aventri_object['responses'], dict):
                responses = aventri_object['responses'].values()
            else:
                responses = aventri_object['responses']

            return {
                'id': 'dit:aventri:Attendee:' + event_id + ':Create',
                'published': now,
                'type': 'dit:aventri:Attendee',
                'dit:application': 'aventri',
                'object': {
                    'attributedTo': {
                        'type': 'dit:aventri:Event',
                        'id': f'dit:aventri:Event:{event_id}'
                    },
                    'id': 'dit:aventri:Attendee:' + aventri_object['attendeeid'],
                    'published': datetime.datetime.strptime(
                        aventri_object['created'], '%Y-%m-%d %H:%M:%S'
                    ).isoformat(),
                    'type': ['dit:aventri:Attendee'],
                    'dit:aventri:approvalstatus': aventri_object['approvalstatus'],
                    'dit:aventri:category': aventri_object['category']['name']
                    if aventri_object['category'] else None,
                    'dit:aventri:createdby': aventri_object['createdby'],
                    'dit:aventri:language': aventri_object['language'],
                    'dit:aventri:lastmodified': aventri_object['lastmodified'],
                    'dit:aventri:modifiedby': aventri_object['modifiedby'],
                    'dit:aventri:registrationstatus': aventri_object['registrationstatus'],
                    'dit:aventri:responses': [{'name': r['name'], 'response': r['response']}
                                              for r in responses]
                }
            }

        def flatten(items):
            for item in items:
                if isinstance(item, Sequence):
                    yield from flatten(item)
                else:
                    yield item
        return (
            map_to_activity(page_event['eventid'], aventri_object)
            for page_event in page_of_events
            for aventri_object in flatten([
                await get_event(page_event['eventid']),
                await get_attendees(page_event['eventid'])
            ]) if aventri_object
        )


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
        now = datetime.datetime.now()
        six_weeks_ago = (now - datetime.timedelta(days=42)).strftime('%Y-%m-%d 00:00:00')
        if ingest_type == 'full':
            timestamp = six_weeks_ago

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
            year, time = campaign['start_ts'].split(' ')
            timestamp = f'{year}T{time}'
            return {
                'type': 'dit:maxemail:Campaign',
                'id': 'dit:maxemail:Campaign:' + email_campaign_id,
                'name': campaign['name'],
                'content': campaign['description'],
                'dit:emailSubject': campaign['subject_line'],
                'dit:maxemail:Campaign:id': int(email_campaign_id),
                'published': timestamp
            }, {
                'type': ['Organization', 'dit:maxemail:Sender'],
                'id': 'dit:maxemail:Sender:' + campaign['from_address'],
                'name': campaign['from_address_alias'],
                'dit:emailAddress': campaign['from_address'],
            }

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

        def common(campaign_id, timestamp, email_address):
            year, time = timestamp.split(' ')
            timestamp = f'{year}T{time}'
            line_id = f'{campaign_id}:{timestamp}:{email_address}'
            return line_id, timestamp

        async def gen_sent_activities_and_timestamp(csv_lines):
            async for line in csv_lines:
                line_id, timestamp = common(line['email id'], line['sent timestamp'],
                                            line['email address'])
                activity = {
                    'id': 'dit:maxemail:Email:Sent:' + line_id + ':Create',
                    'type': 'Create',
                    'dit:application': 'maxemail',
                    'published': timestamp,
                    'object': {
                        'type': ['dit:maxemail:Email', 'dit:maxemail:Email:Sent'],
                        'id': 'dit:maxemail:Email:Sent:' + line_id,
                        'dit:emailAddress': line['email address'],
                        'attributedTo': (await get_email_campaign(line['email id']))[0]
                    }
                }
                yield activity, timestamp

        async def gen_bounced_activities_and_timestamp(csv_lines):
            async for line in csv_lines:
                line_id, timestamp = common(line['email id'],
                                            line['bounce timestamp'], line['email address'])
                activity = {
                    'id': 'dit:maxemail:Email:Bounced:' + line_id + ':Create',
                    'type': 'Create',
                    'dit:application': 'maxemail',
                    'published': timestamp,
                    'object': {
                        'type': ['dit:maxemail:Email', 'dit:maxemail:Email:Bounced'],
                        'id': 'dit:maxemail:Email:Bounced:' + line_id,
                        'dit:emailAddress': line['email address'],
                        'content': line['bounce reason'],
                        'attributedTo': (await get_email_campaign(line['email id']))[0]
                    }
                }
                yield activity, timestamp

        async def gen_opened_activities_and_timestamp(csv_lines):
            async for line in csv_lines:
                line_id, timestamp = common(line['email id'],
                                            line['open timestamp'], line['email address'])
                activity = {
                    'id': 'dit:maxemail:Email:Opened:' + line_id + ':Create',
                    'type': 'Create',
                    'dit:application': 'maxemail',
                    'published': timestamp,
                    'object': {
                        'type': ['dit:maxemail:Email', 'dit:maxemail:Email:Opened'],
                        'id': 'dit:maxemail:Email:Opened:' + line_id,
                        'dit:emailAddress': line['email address'],
                        'attributedTo': (await get_email_campaign(line['email id']))[0]
                    }
                }
                yield activity, timestamp

        async def gen_clicked_activities_and_timestamp(csv_lines):
            async for line in csv_lines:
                line_id, timestamp = common(line['email id'], line['click timestamp'],
                                            line['email address'])
                activity = {
                    'id': 'dit:maxemail:Email:Clicked:' + line_id + ':Create',
                    'type': 'Create',
                    'dit:application': 'maxemail',
                    'published': timestamp,
                    'object': {
                        'type': ['dit:maxemail:Email', 'dit:maxemail:Email:Clicked'],
                        'id': 'dit:maxemail:Email:Clicked:' + line_id,
                        'dit:emailAddress': line['email address'],
                        'url': line['url'],
                        'attributedTo': (await get_email_campaign(line['email id']))[0]
                    }
                }
                yield activity, timestamp

        async def gen_responded_activities_and_timestamp(csv_lines):
            async for line in csv_lines:
                # The column _is_ called "click timestamp" for responded
                line_id, timestamp = common(line['email id'], line['click timestamp'],
                                            line['email address'])
                activity = {
                    'id': 'dit:maxemail:Email:Responded:' + line_id + ':Create',
                    'type': 'Create',
                    'dit:application': 'maxemail',
                    'published': timestamp,
                    'object': {
                        'type': ['dit:maxemail:Email', 'dit:maxemail:Email:Responded'],
                        'id': 'dit:maxemail:Email:Responded:' + line_id,
                        'dit:emailAddress': line['email address'],
                        'attributedTo': (await get_email_campaign(line['email id']))[0]
                    }
                }
                yield activity, timestamp

        async def gen_unsubscribed_activities_and_timestamp(csv_lines):
            async for line in csv_lines:
                line_id, timestamp = common(line['email id'], line['unsubscribe timestamp'],
                                            line['email address'])
                activity = {
                    'id': 'dit:maxemail:Email:Unsubscribed:' + line_id + ':Create',
                    'type': 'Create',
                    'dit:application': 'maxemail',
                    'published': timestamp,
                    'object': {
                        'type': ['dit:maxemail:Email', 'dit:maxemail:Email:Unsubscribed'],
                        'id': 'dit:maxemail:Email:Unsubscribed:' + line_id,
                        'dit:emailAddress': line['email address'],
                        'attributedTo': (await get_email_campaign(line['email id']))[0]
                    }
                }
                yield activity, timestamp

        async def gen_campains_activities_and_timestamp(campaigns, timestamp):
            for campaign_obj, campaign_sender in campaigns.values():
                yield {
                    'id': campaign_obj['id'] + ':Create',
                    'type': 'Create',
                    'dit:application': 'maxemail',
                    'published': campaign_obj['published'],
                    'object': campaign_obj,
                    'actor': campaign_sender
                }, timestamp

        async def multiplex(aiter_initial_timestamps):
            timestamps = [
                initial_timestamp
                for _, initial_timestamp in aiter_initial_timestamps
            ]

            while True:
                at_least_one_success = False

                for i, (aiter, _) in enumerate(aiter_initial_timestamps):
                    try:
                        activity, activity_timestamp = await aiter.__anext__()
                    except StopAsyncIteration:
                        continue
                    else:
                        at_least_one_success = True
                        timestamps[i] = activity_timestamp
                        yield activity, '--'.join(timestamps)

                if not at_least_one_success:
                    break

        async def paginate(page_size, objs):
            page = []
            timestamp = None
            async for obj, timestamp in objs:
                page.append(obj)
                if len(page) == page_size:
                    yield page, timestamp
                    page = []

            if page:
                yield page, timestamp

        def get_with_default(items, index, default):
            try:
                return items[index]
            except IndexError:
                return default

        timestamps = timestamp.split('--')
        timestamp_sent = get_with_default(timestamps, 0, six_weeks_ago)
        timestamp_bounced = get_with_default(timestamps, 1, timestamp_sent)
        timestamp_opened = get_with_default(timestamps, 2, timestamp_bounced)
        timestamp_clicked = get_with_default(timestamps, 3, timestamp_opened)
        timestamp_responded = get_with_default(timestamps, 4, timestamp_clicked)
        timestamp_unsubscribed = get_with_default(timestamps, 5, timestamp_responded)

        sent_data_export_key = await get_data_export_key(timestamp_sent, 'sent')
        sent_csv_lines = gen_data_export_csv(sent_data_export_key)
        sent_activities_and_timestamp = gen_sent_activities_and_timestamp(sent_csv_lines)

        bounced_data_export_key = await get_data_export_key(timestamp_bounced, 'bounced')
        bounced_csv_lines = gen_data_export_csv(bounced_data_export_key)
        bounced_activities_and_timestamp = gen_bounced_activities_and_timestamp(bounced_csv_lines)

        opened_data_export_key = await get_data_export_key(timestamp_opened, 'opened')
        opened_csv_lines = gen_data_export_csv(opened_data_export_key)
        opened_activities_and_timestamp = gen_opened_activities_and_timestamp(opened_csv_lines)

        clicked_data_export_key = await get_data_export_key(timestamp_clicked, 'clicked')
        clicked_csv_lines = gen_data_export_csv(clicked_data_export_key)
        clicked_activities_and_timestamp = gen_clicked_activities_and_timestamp(clicked_csv_lines)

        responded_data_export_key = await get_data_export_key(timestamp_clicked, 'responded')
        responded_csv_lines = gen_data_export_csv(responded_data_export_key)
        responded_activities_and_timestamp = \
            gen_responded_activities_and_timestamp(responded_csv_lines)

        unsubscribed_data_export_key = await get_data_export_key(timestamp_unsubscribed,
                                                                 'unsubscribed')
        unsubscribed_csv_lines = gen_data_export_csv(unsubscribed_data_export_key)
        unsubscribed_activities_and_timestamp = gen_unsubscribed_activities_and_timestamp(
            unsubscribed_csv_lines)

        multiplexed_activities_and_timestamps = multiplex([
            (sent_activities_and_timestamp, timestamp_sent),
            (bounced_activities_and_timestamp, timestamp_bounced),
            (opened_activities_and_timestamp, timestamp_opened),
            (clicked_activities_and_timestamp, timestamp_clicked),
            (responded_activities_and_timestamp, timestamp_responded),
            (unsubscribed_activities_and_timestamp, timestamp_unsubscribed),
        ])

        multiplexed_activity_pages_and_timestamp = paginate(
            feed.page_size, multiplexed_activities_and_timestamps)
        async for activity_page, timestamp in multiplexed_activity_pages_and_timestamp:
            yield activity_page, timestamp

        campaigns_activities_and_timestamp = gen_campains_activities_and_timestamp(
            campaigns, timestamp)
        campaigns_activity_pages_and_timestamp = paginate(
            feed.page_size, campaigns_activities_and_timestamp)
        async for activity_page, timestamp in campaigns_activity_pages_and_timestamp:
            yield activity_page, timestamp
