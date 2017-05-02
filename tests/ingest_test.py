import http.client
import unittest

from multiprocessing import Process

import time

from ingest.streams.context import init_streams_context
from settings.ingestservers import init_servers

TESTING_GUARDIAN_HOSTNAME = 'localhost'
TESTING_GUARDIAN_PORT = 8000
TESTING_MONETDB_HOSTNAME = 'localhost'
TESTING_MONETDB_PORT = 50000
TESTING_MONETDB_DATABASE = 'testing'


def init_guardian_server():
    init_streams_context(TESTING_MONETDB_HOSTNAME, TESTING_MONETDB_PORT, 'monetdb', 'monetdb', TESTING_MONETDB_DATABASE)
    init_servers(TESTING_GUARDIAN_PORT, 1833)


class IngestTest(unittest.TestCase):
    Guardian_Servers = None
    HTTP_Client = None

    @classmethod
    def setUpClass(cls):
        cls.Guardian_Servers = Process(target=init_guardian_server)
        cls.Guardian_Servers.start()
        time.sleep(2)
        cls.HTTP_Client = http.client.HTTPConnection(TESTING_GUARDIAN_HOSTNAME, TESTING_GUARDIAN_PORT)

    def test_create_stream_json(self):
        insert_string = """
        {
            "schema": "sys",
            "stream": "testing",
            "flushing": {"base": "auto"},
            "columns": [
                {"name": "a", "type": "int", "nullable": true},
                {"name": "b", "type": "text", "nullable": false}
            ]
        }
        """
        IngestTest.HTTP_Client.request('POST', '/context', body=insert_string)
        response = IngestTest.HTTP_Client.getresponse()
        response.read()
        self.assertEqual(response.status, 201)

    def test_insert_data(self):
        insert_string = """
        [{"schema":"sys","stream":"testing","values":[{"a":100,"b":"cool"},{"a":2,"b":"lekker"},{"a":3,"b":"mmm"}]}]
        """
        IngestTest.HTTP_Client.request('POST', '/json', body=insert_string)
        response = IngestTest.HTTP_Client.getresponse()
        response.read()
        self.assertEqual(response.status, 201)


    def test_delete_stream_json(self):
        insert_string = """
        {
            "schema": "sys",
            "stream": "testing"
        }
        """
        IngestTest.HTTP_Client.request('DELETE', '/context', body=insert_string)
        response = IngestTest.HTTP_Client.getresponse()
        response.read()
        self.assertEqual(response.status, 204)

    @classmethod
    def tearDownClass(cls):
        cls.HTTP_Client.close()
        cls.Guardian_Servers.terminate()
