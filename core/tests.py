from django.test import TestCase
from rest_framework.parsers import JSONParser
from six import BytesIO

from subprocess import Popen
import unittest
import urllib


class AddActionTests(TestCase):

    def test_invalid_action_request_due_to_missing_type(self):
        raw_json = """
                {
                   "source": "directory",
                   "occurrence_date": "2012-04-23T18:25:43.511Z",
                   "actor_name": "Bob Roberts",
                   "actor_email_address": "bob.roberts@somewhere.io",
                   "actor_business_sso_uri": "http://somewhere.org/something",
                   "details": [
                      {
                         "key": "originating_page",
                         "value": "Direct request"
                      },
                      {
                         "key": "message",
                         "value": "Lorem ipsum dolor sit amet, consectetur adipiscing elit"
                      }
                   ]
                }
                """

        url_for_adding_an_action = f"/api/actions/"
        response_from_adding_an_action = self.client.post(url_for_adding_an_action, data=raw_json,
                                                          content_type='application/json')
        self.assertEqual(404, response_from_adding_an_action.status_code)

    def test_valid_but_minimal_action_request(self):
        raw_json = """
           {
                "details":[]
           }
           """

        url_for_adding_an_action = f"/api/actions/some_type/"
        response_from_adding_an_action = self.client.post(url_for_adding_an_action, data=raw_json,
                                                          content_type='application/json')
        self.assertEqual(201, response_from_adding_an_action.status_code)
        url_for_location_of_new_action = response_from_adding_an_action['Location']
        expected_new_action_id = 2
        self.assertEqual(f"http://testserver/api/actions/{expected_new_action_id}", url_for_location_of_new_action)

        response_from_getting_an_action = self.client.get(url_for_location_of_new_action)
        returned_action_data = JSONParser().parse(BytesIO(response_from_getting_an_action.content))

        self.assertEqual(200, response_from_getting_an_action.status_code)

        self.assertEqual(None, returned_action_data['actor_name'])
        self.assertEqual(None, returned_action_data['actor_email_address'])
        self.assertEqual(None, returned_action_data['actor_business_sso_uri'])
        self.assertEqual(expected_new_action_id, returned_action_data['id'])
        self.assertEqual(None, returned_action_data['occurrence_date'])
        self.assertIsNotNone(returned_action_data['recording_date'])
        self.assertEqual(None, returned_action_data['source'])

        action_type = returned_action_data['action_type']
        self.assertEqual(2, action_type['id'])
        self.assertEqual("some_type", action_type['name'])

        action_details = returned_action_data['action_details']
        self.assertEqual(0, len(action_details))

    def test_valid_action_request_with_details(self):
        raw_json = """
        {
           "source": "directory",
           "occurrence_date": "2012-04-23T18:25:43.511Z",
           "actor_name": "Bob Roberts",
           "actor_email_address": "bob.roberts@somewhere.io",
           "actor_business_sso_uri": "http://somewhere.org/something",
           "details": [
              {
                 "key": "originating_page",
                 "value": "Direct request"
              },
              {
                 "key": "message",
                 "value": "Lorem ipsum dolor sit amet, consectetur adipiscing elit"
              }
           ]
        }
        """

        url_for_adding_an_action = "/api/actions/some_other_type/"
        response_from_adding_an_action = self.client.post(url_for_adding_an_action, data=raw_json,
                                                          content_type='application/json')
        self.assertEqual(201, response_from_adding_an_action.status_code)
        url_for_location_of_new_action = response_from_adding_an_action['Location']
        expected_new_action_id = 1
        self.assertEqual(f"http://testserver/api/actions/{expected_new_action_id}", url_for_location_of_new_action)

        response_from_getting_an_action = self.client.get(url_for_location_of_new_action)
        returned_action_data = JSONParser().parse(BytesIO(response_from_getting_an_action.content))

        self.assertEqual(200, response_from_getting_an_action.status_code)

        self.assertEqual("Bob Roberts", returned_action_data['actor_name'])
        self.assertEqual("bob.roberts@somewhere.io", returned_action_data['actor_email_address'])
        self.assertEqual("http://somewhere.org/something", returned_action_data['actor_business_sso_uri'])
        self.assertEqual(expected_new_action_id, returned_action_data['id'])
        self.assertEqual("2012-04-23T18:25:43.511000Z", returned_action_data['occurrence_date'])
        self.assertIsNotNone(returned_action_data['recording_date'])
        self.assertEqual("directory", returned_action_data['source'])

        action_type = returned_action_data['action_type']
        self.assertEqual(1, action_type['id'])
        self.assertEqual("some_other_type", action_type['name'])

        action_details = returned_action_data['action_details']

        self.assertEqual(2, len(action_details))
        self.assertEqual("originating_page", action_details[0]['key'])
        self.assertEqual("Direct request", action_details[0]['value'])
        self.assertEqual("message", action_details[1]['key'])
        self.assertEqual("Lorem ipsum dolor sit amet, consectetur adipiscing elit", action_details[1]['value'])


class TestServer(unittest.TestCase):

    def setUp(self):
      self.server = Popen(["gunicorn", "conf.wsgi", "--config", "conf/gunicorn.py"])

    def tearDown(self):
      self.server.kill()

    def test_server_accepts_http(self):
      def is_http_accepted():
          try:
            urllib.request.urlopen('http://localhost:8000', timeout=1)
            return True
          except urllib.request.URLError as e:
            return 'nodename nor servname provided, or not known' not in str(e.reason)
      self.assertTrue(is_http_accepted())
