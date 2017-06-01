from tornado.web import Application

from settings import settings
from tshttp.ingest.ingestinfluxdb import InfluxDBInput, InfluxDBDiscovery
from tshttp.ingest.ingestjson import StreamsHandling, JSONInput, StreamInfo
from tshttp.queries.query import QueryHandler

# guardian_application = Application([
#     (r"/query", QueryHandler),
#     (r"/context", StreamsHandling),
#     (r"/stream/([a-zA-Z0-9]+)/([a-zA-Z0-9]+)", StreamInfo),
#     (r"/json", JSONInput),
#     (r"/influxdb", InfluxDBInput),
#     (r"/discovery", InfluxDBDiscovery),
# ], settings)

def createApp(options):
    app = Application([
        (r"/query", QueryHandler, options),
        (r"/context", StreamsHandling),
        (r"/stream/([a-zA-Z0-9]+)/([a-zA-Z0-9]+)", StreamInfo, options),
        (r"/json", JSONInput),
        (r"/influxdb", InfluxDBInput),
        (r"/discovery", InfluxDBDiscovery),
    ], settings)
    return app

