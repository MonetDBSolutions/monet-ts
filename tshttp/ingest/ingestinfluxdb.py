import asyncio
import json
import tornado.web

from jsonschema import ValidationError
from ingest.inputs import influxdb
from ingest.monetdb.naming import THREAD_POOL
from ingest.streams.context import get_streams_context


class InfluxDBHandler(tornado.web.RequestHandler):
    """Request handler for InfluxDB ingestion"""

    def data_received(self, chunk):
        pass

    def write_error(self, status_code, **kwargs):
        if 'message' not in kwargs:
            if status_code == 405:
                kwargs['message'] = 'Invalid HTTP method.'
            else:
                kwargs['message'] = 'Unknown error.'

        self.write(json.dumps(kwargs))
        self.set_status(status_code)


class InfluxDBInput(InfluxDBHandler):
    """Ingest a single point under the InfluxDB protocol"""

    async def post(self):  # add data to a stream
        try: # TODO check UTF-8 strings?
            schema, stream, tags, values = influxdb.parse_influxdb_line(self.request.body.decode("utf-8"))
        except BaseException as ex:
            self.write_error(400, **{'message': ex.__str__()})
            return

        try:  # check if stream exists, if not return 404
            stream = get_streams_context().get_existing_stream(schema, stream)
        except BaseException as ex:
            self.write_error(404, **{'message': ex.__str__()})
            return

        try:  # validate and insert data, if not return 400
            await asyncio.wrap_future(THREAD_POOL.submit(stream.insert_json, [values]))
        except (ValidationError, BaseException) as ex:
            self.write_error(400, **{'message': ex.__str__()})
            return

        self.set_status(201)
