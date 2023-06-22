from abc import ABCMeta, abstractmethod
import asyncio
import csv
import datetime
from distutils.util import strtobool    # pylint: disable=import-error, no-name-in-module
from json import JSONDecodeError
import re
from io import TextIOWrapper, BytesIO
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
from .metrics import (
    metric_timer,
)
from .utils import (
    json_loads,
    sub_dict_lower,
)
from .utils import (
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
    down_grace_period = 60 * 60 * 48

    full_ingest_page_interval = 0.25
    full_ingest_interval = 120
    updates_page_interval = 1
    exception_intervals = [1, 2, 4, 8, 16, 32, 64]

    disable_updates = False  # this is to disable updates feed, if necessary

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

    down_grace_period = 60 * 60 * 24 * 3  # Full ingest takes ~2 days, if nothing after 3, alert
    full_ingest_page_interval = 0  # There are sleeps in tht HTTP requests in this class
    full_ingest_interval = 60 * 60

    updates_page_interval = 60 * 60 * 24 * 30
    exception_intervals = [120, 180, 240, 300]

    # This is quite small so even when we have a lot of sleeps, we still have signs that the
    # feed is working in Grafana
    ingest_page_size = 20

    disable_updates = True

    @classmethod
    def parse_config(cls, config):
        return cls(
            **sub_dict_lower(
                config, ['UNIQUE_ID', 'SEED', 'ACCOUNT_ID', 'API_KEY', 'AUTH_URL',
                         'ATTENDEES_LIST_URL', 'EVENT_QUESTIONS_LIST_URL', 'SESSIONS_LIST_URL',
                         'SESSION_REGISTRATIONS_LIST_URL']))

    def __init__(self, unique_id, seed, account_id, api_key, auth_url, attendees_list_url,
                 event_questions_list_url, sessions_list_url, session_registrations_list_url):
        self.lock = asyncio.Lock()
        self.unique_id = unique_id
        self.seed = seed
        self.account_id = account_id
        self.api_key = api_key
        self.auth_url = auth_url
        self.attendees_list_url = attendees_list_url
        self.event_questions_list_url = event_questions_list_url
        self.sessions_list_url = sessions_list_url
        self.session_registrations_list_url = session_registrations_list_url
        self.accesstoken = None

    @staticmethod
    def next_href(_):
        """ aventri API does not support pagination
            returns (None)
        """
        return None

    async def auth_headers(self, _, __):
        return {}

    async def generate_access_token(self, context):
        context.logger.info('Making aventri auth request to %s', self.auth_url)
        result = await http_make_request(
            context.session, context.metrics, 'GET', self.auth_url,
            data=b'',
            params=(
                ('accountid', str(self.account_id)),
                ('key', str(self.api_key)),
            ),
            headers={},
        )
        result.raise_for_status()
        accesstoken = str(json_loads(result._body)['accesstoken'])
        context.logger.info('fresh access token %s', accesstoken)
        return accesstoken

    async def http_make_auth_request(self, context, method, url, data, params=()):
        # Aventri DS APIs tokens have short expiry
        num_attempts = 0
        max_attempts = 5
        while True:
            try:
                num_attempts += 1
                if self.accesstoken is None:
                    self.accesstoken = await self.generate_access_token(context)
                params_auth = params + (('accesstoken', str(self.accesstoken)),)
                context.logger.info('Making aventri request to %s with params %s', url, params)
                result = await http_make_request(
                    context.session, context.metrics, method, url,
                    data=data, headers={}, params=params_auth,
                )
                error_status = json_loads(result._body).get('status', None)
                error_msg = json_loads(result._body).get('msg', None)
                if error_status == 'error' and error_msg.startswith('Not authorized to access'):
                    raise aiohttp.ClientResponseError(
                        result.request_info, result.history, status=403)
            except (aiohttp.ClientResponseError, JSONDecodeError):
                if num_attempts < max_attempts:
                    self.accesstoken = None
                    continue
            return result

    async def http_make_aventri_request(
            self, context, method, url, data, sleep_interval=0.5, params=()):
        logger = context.logger

        num_attempts = 0
        max_attempts = 10
        retry_interval = 30

        while True:
            num_attempts += 1
            retry_interval += 60
            try:
                result = await self.http_make_auth_request(context, method, url, data, params)
                result.raise_for_status()

                if sleep_interval:
                    await sleep(context, sleep_interval)
                results = json_loads(result._body).get('ResultSet', [])
                return results
            except aiohttp.ClientResponseError as client_error:
                if (num_attempts >= max_attempts or client_error.status not in [
                        429, 502, 504,
                ]):
                    raise
                logger.debug(
                    'aventri: HTTP %s received at attempt (%s). Will retry after (%s) seconds',
                    client_error.status,
                    num_attempts,
                    retry_interval,
                )
                context.raven_client.captureMessage(
                    f'aventri: HTTP {client_error.status} received at attempt ({num_attempts}).'
                    f'Will retry after ({retry_interval}) seconds',
                )
                await sleep(context, retry_interval)
            except JSONDecodeError:
                if num_attempts >= max_attempts:
                    logger.debug(
                        'aventri: JSONDecodeError at attempt (%s). Will retry after (%s) seconds',
                        num_attempts,
                        retry_interval,
                    )
                    await sleep(context, retry_interval)

    async def pages(self, context, feed, href, ingest_type):
        # pylint: disable=too-many-statements
        logger = context.logger

        async def gen_activities(href):
            with \
                    logged(logger.info, logger.warning, 'Polling page (%s)',
                           [href]), \
                    metric_timer(context.metrics['ingest_page_duration_seconds'],
                                 [feed.unique_id, ingest_type, 'pull']):

                async for event in gen_events():
                    yield self.map_to_event_activity(event)

                    async for attendee in gen_attendees(event):
                        yield self.map_to_attendee_activity(attendee, event)

                    async for session in gen_sessions(event):
                        yield self.map_to_session_activity(session, event)

                    async for registration in gen_registrations(event):
                        yield self.map_to_registration_activity(registration, event)

        # async def gen_event_questions(event_id):
        #     response = await self.http_make_aventri_request(
        #         context,
        #         'GET',
        #         self.event_questions_list_url.format(event_id=event_id),
        #         data=b'',
        #     )
        #     return [question['ds_fieldname'] for question in response.values()]

        async def gen_events():
            next_page = 1
            # default is 1024, but keep it low as it becomes slow
            page_size = 100
            while True:
                params = (
                    ('pageNumber', str(next_page)),
                    ('pageSize', str(page_size)),
                )

                page_of_events = await self.http_make_aventri_request(
                    context, 'GET', self.seed, data=b'', params=params,
                )
                for event in page_of_events:
                    
                    event['question'] = None
                    # If events are deleted, a request for questions returns a 500
                    # But also, some other non-deleted events fail as well, ones
                    # that have many null values. We arbitrarily choose url as the
                    # sensor for this state
                    # event['questions'] = \
                    #     await gen_event_questions(event['eventid']) \
                    #     if event['event_deleted'] == '0' and event['url'] is not None else None
                    yield event

                if len(page_of_events) != page_size:
                    break

                next_page += 1

        async def gen_attendees(event):
            logger = context.logger
            url = self.attendees_list_url.format(event_id=event['eventid'])

            next_page = 1
            # Be careful of bigger: sometimes is very slow
            page_size = 100
            while True:
                params = (
                    ('pageNumber', str(next_page)),
                    ('pageSize', str(page_size)),
                )
                with logged(logger.debug, logger.warning, 'Fetching attendee list', []):
                    attendees = await self.http_make_aventri_request(
                        context, 'GET', url, data=b'', params=params,
                    )
                for attendee in attendees:
                    yield attendee

                if len(attendees) != page_size:
                    break

                next_page += 1

        async def gen_sessions(event):
            logger = context.logger
            url = self.sessions_list_url.format(event_id=event['eventid'])

            next_page = 1
            # Be careful of bigger: sometimes is very slow
            page_size = 100
            while True:
                params = (
                    ('pageNumber', str(next_page)),
                    ('pageSize', str(page_size)),
                )
                with logged(logger.debug, logger.warning, 'Fetching sessions list', []):
                    sessions = await self.http_make_aventri_request(
                        context, 'GET', url, data=b'', params=params,
                    )
                for session in sessions:
                    yield session

                if len(sessions) != page_size:
                    break

                next_page += 1

        async def gen_registrations(event):
            logger = context.logger
            url = self.session_registrations_list_url.format(event_id=event['eventid'])

            next_page = 1
            # Be careful of bigger: sometimes is very slow
            page_size = 100
            while True:
                params = (
                    ('pageNumber', str(next_page)),
                    ('pageSize', str(page_size)),
                )
                with logged(logger.debug, logger.warning, 'Fetching sessions list', []):
                    sessions = await self.http_make_aventri_request(
                        context, 'GET', url, data=b'', params=params,
                    )
                for session in sessions:
                    yield session

                if len(sessions) != page_size:
                    break

                next_page += 1

        async def paginate(items):
            # pylint: disable=undefined-loop-variable
            page_size = self.ingest_page_size
            current = []
            async for item in items:
                current.append(item)

                while len(current) >= page_size:
                    to_yield, current = current[:page_size], current[page_size:]
                    yield to_yield

            if current:
                yield current

        activities = gen_activities(href)
        pages = paginate(activities)

        async for page in pages:
            yield page, href

    def map_to_event_activity(self, event):
        event_id = event['eventid']
        folder_name = event['folderpath'].split('/')[-1]
        published = self.format_datetime(event['event_created'])

        return {
            'id': 'dit:aventri:Event:' + event_id + ':Create',
            'published': published,
            'type': 'dit:aventri:Event',
            'dit:application': 'aventri',
            'object': {
                'id': 'dit:aventri:Event:' + event_id,
                'name': event['eventname'],
                'published': published,
                'updated': self.format_datetime(event['event_lastmodified']),
                # The following mappings are used to allow great.gov.uk
                # search to filter on events.
                'attributedTo': {
                    'type': 'dit:aventri:Folder',
                    'id': f'dit:aventri:Folder:{folder_name}'
                },
                'content': event['event_description'],
                'dit:public': bool(strtobool(event['include_calendar'])),
                'dit:status': event['eventstatus'],
                'endTime': self.format_date_and_time(
                    event['enddate'], event['endtime'],
                ),
                'startTime': self.format_date_and_time(
                    event['startdate'], event['starttime'],
                ),
                'type': ['dit:aventri:Event'] + (
                    ['Tombstone'] if event['event_deleted'] == '1' else []
                ),
                'url': event['url'],
                'dit:aventri:approval_required': event['approval_required'],
                'dit:aventri:approval_status': event['approval_status'],
                'dit:aventri:city': event['event_city'],
                'dit:aventri:clientcontact': event['clientcontact'],
                'dit:aventri:closedate': self.format_date_and_time(
                    event['closedate'], event['closetime'],
                ),
                'dit:aventri:code': event['code'],
                'dit:aventri:contactinfo': event['contactinfo'],
                'dit:aventri:country': event['event_country'],
                'dit:aventri:createdby': event['createdby'],
                'dit:aventri:defaultlanguage': event['defaultlanguage'],
                'dit:aventri:folderid': event['folderid'],
                'dit:aventri:live_date': self.format_date(event['live_date']),
                'dit:aventri:location_address1': event['event_loc_addr1'],
                'dit:aventri:location_address2': event['event_loc_addr2'],
                'dit:aventri:location_address3': event['event_loc_addr3'],
                'dit:aventri:location_city': event['event_loc_city'],
                'dit:aventri:location_country': event['event_loc_country'],
                'dit:aventri:location_name': event['event_loc_name'],
                'dit:aventri:location_postcode': event['event_loc_postcode'],
                'dit:aventri:location_state': event['event_loc_state'],
                'dit:aventri:locationname': event['locationname'],
                'dit:aventri:max_reg': event['max_reg'],
                'dit:aventri:modifiedby': event['modifiedby'],
                'dit:aventri:modifieddatetime': self.format_datetime(event['event_lastmodified']),
                'dit:aventri:price_type': event['price_type'],
                'dit:aventri:standardcurrency': event['standardcurrency'],
                'dit:aventri:state': event['event_state'],
                'dit:aventri:timezone': event['timezone_name'],
            }
        }

    def map_to_attendee_activity(self, attendee, event):
        event_id = event['eventid']
        attendee_id = attendee['attendeeid']
        return {
            'id': 'dit:aventri:Event:' + event_id + ':Attendee:' + attendee_id + ':Create',
            'published': self.format_datetime(attendee['created']),
            'type': 'dit:aventri:Attendee',
            'dit:application': 'aventri',
            'object': {
                'attributedTo': {
                    'type': 'dit:aventri:Event',
                    'id': f'dit:aventri:Event:{event_id}'
                },
                'id': 'dit:aventri:Attendee:' + attendee_id,
                'published': self.format_datetime(attendee['created']),
                'type': ['dit:aventri:Attendee'],
                'dit:aventri:approvalstatus': attendee['approval_status'],
                'dit:aventri:category': attendee['category_shortname'],
                'dit:aventri:createdby': attendee['createdby'],
                'dit:aventri:language': attendee['language'],
                'dit:aventri:lastmodified': self.format_datetime(attendee['lastmodified']),
                'dit:aventri:modifiedby': attendee['modifiedby'],
                'dit:aventri:registrationstatus': attendee['registrationstatus'],
                'dit:aventri:email': attendee['email'],
                'dit:aventri:firstname': attendee['fname'],
                'dit:aventri:lastname': attendee['lname'],
                'dit:aventri:companyname': attendee.get('company', None),
                'dit:aventri:virtualeventattendance': attendee['virtual_event_attendance'],
                'dit:aventri:lastlobbylogin': self.format_datetime(
                    attendee.get('last_lobby_login', None)
                ),
                'dit:aventri:attendeeQuestions': {
                    question: attendee.get(question)
                    for question in event['questions']
                } if event['questions'] is not None else None,
                # although dups, below fields are used by datahub crm
                'dit:emailAddress': attendee['email'],
                'dit:firstName': attendee['fname'],
                'dit:lastName': attendee['lname'],
                'dit:registrationStatus': attendee['registrationstatus'],
                'dit:companyName': attendee['company']
            }
        }

    def map_to_session_activity(self, session, event):
        event_id = event['eventid']
        session_id = session['sessionid']
        published = self.format_datetime(event['event_created'])
        return {
            'id': 'dit:aventri:Event:' + event_id + ':Session:' + session_id + ':Create',
            'published': published,
            'type': 'dit:aventri:Session',
            'dit:application': 'aventri',
            'object': {
                'attributedTo': {
                    'type': 'dit:aventri:Event',
                    'id': f'dit:aventri:Event:{event_id}'
                },
                'id': 'dit:aventri:Session:' + session_id,
                'published': published,
                'type': ['dit:aventri:Session'],
                'dit:aventri:starttime': session['starttime'],
                'dit:aventri:endtime': session['endtime'],
                'dit:aventri:name': session['name'],
                'dit:aventri:desc': session['desc'],
            }
        }

    def map_to_registration_activity(self, registration, event):
        event_id = event['eventid']
        session_id = registration['sessionid']
        attendee_id = registration['attendeeid']
        published = self.format_datetime(event['event_created'])
        as_id = ':Session:' + session_id + ':Attendee:' + attendee_id
        return {
            'id': 'dit:aventri:Event:' + event_id + as_id + ':Create',
            'published': published,
            'type': 'dit:aventri:SessionRegistration',
            'dit:application': 'aventri',
            'object': {
                'attributedTo': {
                    'type': 'dit:aventri:Event',
                    'id': f'dit:aventri:Event:{event_id}'
                },
                'id': 'dit:aventri:Session:' + session_id + ':Attendee:' + attendee_id,
                'published': published,
                'type': ['dit:aventri:SessionRegistration'],
                'dit:aventri:session_id': session_id,
                'dit:aventri:attendee_id': attendee_id,
                'dit:aventri:lastmodified': self.format_datetime(registration['lastmodified']),
                'dit:aventri:registration_status': registration['registration_status'],
            }
        }

    @staticmethod
    def format_datetime(aventri_datetime):
        return \
            None if (aventri_datetime is None or aventri_datetime == '0000-00-00 00:00:00') else \
            datetime.datetime.strptime(aventri_datetime, '%Y-%m-%d %H:%M:%S').isoformat()

    @staticmethod
    def format_date_and_time(aventri_date, aventri_time):
        checked_date = aventri_date if aventri_date else '0000-00-00'
        checked_time = aventri_time if aventri_time else '00:00:00'
        if checked_date == '0000-00-00':
            return None

        return datetime.datetime.strptime(
            f'{checked_date} {checked_time}',
            '%Y-%m-%d %H:%M:%S'
        ).isoformat()

    @staticmethod
    def format_date(aventri_datetime):
        return \
            None if (aventri_datetime is None or aventri_datetime == '0000-00-00') else \
            datetime.datetime.strptime(aventri_datetime, '%Y-%m-%d').isoformat()

    @classmethod
    async def get_activities(cls, _, feed):
        pass


class MaxemailFeed(Feed):

    down_grace_period = 60 * 60 * 4

    full_ingest_page_interval = 0.1
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

            for _ in range(0, 5):
                try:
                    with logged(context.logger.debug, context.logger.warning,
                                'maxemail Fetching campaign (%s) with payload (%s)',
                                [url, payload]):
                        result = await http_make_request(
                            context.single_use_session,
                            context.metrics,
                            'POST',
                            url,
                            data=payload,
                            headers=await feed.auth_headers(None, None),
                        )
                        result.raise_for_status()
                except (asyncio.TimeoutError, aiohttp.ClientPayloadError):
                    await asyncio.sleep(10)
                else:
                    break
            if result.status != 200:
                raise Exception(
                    f'Failed fetching maxemail campain {url} with payload {payload}')
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
                            context.single_use_session,
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

                lines_iter = TextIOWrapper(BytesIO((await http_make_request(
                    context.single_use_session,
                    context.metrics,
                    'POST',
                    url,
                    data={},
                    headers=await feed.auth_headers(None, None),
                ))._body), encoding='utf-8', newline='')

                for row in csv.DictReader(lines_iter, skipinitialspace=True, delimiter=',',
                                          quotechar='"', quoting=csv.QUOTE_ALL):
                    yield row

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
        timestamps = [timestamp_sent, timestamp_bounced, timestamp_opened,
                      timestamp_clicked, timestamp_responded, timestamp_unsubscribed]

        sent_data_export_key = await get_data_export_key(timestamp_sent, 'sent')
        sent_csv_lines = gen_data_export_csv(sent_data_export_key)
        sent_activities_and_timestamp = gen_sent_activities_and_timestamp(sent_csv_lines)

        async for activity_page, timestamp in paginate(feed.page_size,
                                                       sent_activities_and_timestamp):
            timestamps[0] = timestamp
            yield activity_page, '--'.join(timestamps)

        bounced_data_export_key = await get_data_export_key(timestamp_bounced, 'bounced')
        bounced_csv_lines = gen_data_export_csv(bounced_data_export_key)
        bounced_activities_and_timestamp = gen_bounced_activities_and_timestamp(bounced_csv_lines)

        async for activity_page, timestamp in paginate(feed.page_size,
                                                       bounced_activities_and_timestamp):
            timestamps[1] = timestamp
            yield activity_page, '--'.join(timestamps)

        opened_data_export_key = await get_data_export_key(timestamp_opened, 'opened')
        opened_csv_lines = gen_data_export_csv(opened_data_export_key)
        opened_activities_and_timestamp = gen_opened_activities_and_timestamp(opened_csv_lines)

        async for activity_page, timestamp in paginate(feed.page_size,
                                                       opened_activities_and_timestamp):
            timestamps[2] = timestamp
            yield activity_page, '--'.join(timestamps)

        clicked_data_export_key = await get_data_export_key(timestamp_clicked, 'clicked')
        clicked_csv_lines = gen_data_export_csv(clicked_data_export_key)
        clicked_activities_and_timestamp = gen_clicked_activities_and_timestamp(clicked_csv_lines)

        async for activity_page, timestamp in paginate(feed.page_size,
                                                       clicked_activities_and_timestamp):
            timestamps[3] = timestamp
            yield activity_page, '--'.join(timestamps)

        responded_data_export_key = await get_data_export_key(timestamp_clicked, 'responded')
        responded_csv_lines = gen_data_export_csv(responded_data_export_key)
        responded_activities_and_timestamp = gen_responded_activities_and_timestamp(
            responded_csv_lines)

        async for activity_page, timestamp in paginate(feed.page_size,
                                                       responded_activities_and_timestamp):
            timestamps[4] = timestamp
            yield activity_page, '--'.join(timestamps)

        unsubscribed_data_export_key = await get_data_export_key(timestamp_unsubscribed,
                                                                 'unsubscribed')
        unsubscribed_csv_lines = gen_data_export_csv(unsubscribed_data_export_key)
        unsubscribed_activities_and_timestamp = gen_unsubscribed_activities_and_timestamp(
            unsubscribed_csv_lines)

        async for activity_page, timestamp in paginate(feed.page_size,
                                                       unsubscribed_activities_and_timestamp):
            timestamps[5] = timestamp
            yield activity_page, '--'.join(timestamps)

        campaigns_activities_and_timestamp = gen_campains_activities_and_timestamp(
            campaigns, '--'.join(timestamps))
        campaigns_activity_pages_and_timestamp = paginate(
            feed.page_size, campaigns_activities_and_timestamp)
        async for activity_page, timestamp in campaigns_activity_pages_and_timestamp:
            yield activity_page, timestamp
