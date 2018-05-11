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
        time.sleep(5)
        self.assertTrue(is_http_accepted())


def is_http_accepted():
    try:
        urllib.request.urlopen('http://127.0.0.1:8080', timeout=1)
        return True
    except urllib.request.URLError as e:
        return ('nodename nor servname provided, or not known' not in str(e.reason)
                and 'Connection refused' not in str(e.reason))
