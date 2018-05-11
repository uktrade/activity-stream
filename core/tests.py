from subprocess import Popen
import time
import unittest
import urllib.request


class TestProcess(unittest.TestCase):

    def setUp(self):
        self.server = Popen(['python', '-m', 'core.app'])

    def tearDown(self):
        self.server.kill()

    def test_server_accepts_http(self):
        self.assertTrue(is_http_accepted())


def is_http_accepted():
    def is_connection_error(e):
        return ('nodename nor servname provided, or not known' in str(e.reason)
                or 'Connection refused' in str(e.reason))

    attempts = 0
    while attempts < 20:
        try:
            urllib.request.urlopen('http://127.0.0.1:8080', timeout=1)
            return True
        except urllib.request.URLError as e:
            attempts += 1
            time.sleep(0.2)
            if not is_connection_error(e):
                return True

    return False
