# -*- coding: UTF-8 -*-
import base64
import unittest
from mock import patch, Mock
from six.moves import urllib

from pandasticsearch.client import RestClient


class TestClients(unittest.TestCase):
    @patch('pandasticsearch.client.urllib.request.urlopen')
    def test_rest_client_returns_results(self, mock_urlopen):
        response = Mock()
        response.read.return_value = """{"hits" : {"hits": [{"_source": {}}] }}""".encode("utf-8")
        mock_urlopen.return_value = response

        client = RestClient("http://localhost:9200")

        json = client.post(data="xxxx")

        print(json)
        self.assertIsNotNone(json)
        self.assertEqual(json, {"hits": {"hits": [{"_source": {}}]}})

    @patch('pandasticsearch.client.urllib.request.urlopen')
    def test_headers_post(self, mock_urlopen):
        client = RestClient("http://localhost:9200", headers={'Authorization': 'Basic SUFtOlRlc3Rpbmc='})
        response = Mock()
        response.read.return_value = """{"hits" : {"hits": [{"_source": {}}] }}""".encode("utf-8")
        mock_urlopen.return_value = response

        client.post(data="test")
        expected_headers = {'Content-type': 'application/json', 'Authorization': 'Basic SUFtOlRlc3Rpbmc='}
        mock_request_headers = mock_urlopen.call_args[0][0].headers
        self.assertEqual(expected_headers, mock_request_headers)

    @patch('pandasticsearch.client.urllib.request.urlopen')
    def test_headers_get(self, mock_urlopen):
        client = RestClient("http://localhost:9200", headers={'Authorization': 'Basic SUFtOlRlc3Rpbmc='})
        response = Mock()
        response.read.return_value = """{"hits" : {"hits": [{"_source": {}}] }}""".encode("utf-8")
        mock_urlopen.return_value = response

        client.get()
        expected_headers = {'Authorization': 'Basic SUFtOlRlc3Rpbmc='}
        mock_request_headers = mock_urlopen.call_args[0][0].headers
        self.assertEqual(expected_headers, mock_request_headers)

if __name__ == '__main__':
    unittest.main()
