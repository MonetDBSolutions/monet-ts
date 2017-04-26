from tornado.web import Application

from settings import settings
from tshttp.ingest.ingestinfluxdb import InfluxDBInput
from tshttp.ingest.ingestjson import StreamsInfo, StreamInput, StreamsHandling
from tshttp.queries.query import QueryHandler

guardian_application = Application([
    (r"/query", QueryHandler),
    (r"/streams", StreamsInfo),
    (r"/stream/([a-zA-Z0-9]+)/([a-zA-Z0-9]+)", StreamInput),
    (r"/context", StreamsHandling),
    (r"/influxdb", InfluxDBInput),
], settings)
