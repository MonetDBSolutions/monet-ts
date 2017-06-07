from tornado.web import Application

from settings import settings
from tshttp.ingest.ingestinfluxdb import InfluxDBInput, InfluxDBDiscovery, InfluxDBDiscoverySlow
from tshttp.ingest.ingestjson import StreamsHandling, JSONInput, StreamInfo
from tshttp.queries.query import QueryHandler

def createApp(options):
    app = Application([
        (r"/query", QueryHandler, options),
        (r"/context", StreamsHandling),
        (r"/stream/([a-zA-Z0-9]+)/([a-zA-Z0-9]+)", StreamInfo, options),
        (r"/json", JSONInput),
        (r"/influxdb", InfluxDBInput),
        (r"/discovery", InfluxDBDiscovery),
        (r"/discoveryslow", InfluxDBDiscoverySlow)
    ], settings)
    return app
