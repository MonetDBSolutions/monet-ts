from tornado.web import Application

from settings import settings
from tshttp.ingest.ingestinfluxdb import InfluxDBInput, InfluxDBDiscovery
from tshttp.ingest.ingestjson import StreamInfo, StreamsHandling, JSONInput
from tshttp.queries.query import QueryHandler

guardian_application = Application([
    (r"/query", QueryHandler),
    (r"/context", StreamsHandling),
    (r"/stream/([a-zA-Z0-9]+)/([a-zA-Z0-9]+)", StreamInfo),
    (r"/json", JSONInput),
    (r"/influxdb", InfluxDBInput),
    (r"/discovery", InfluxDBDiscovery),
], settings)
