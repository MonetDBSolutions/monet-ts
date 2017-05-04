import asyncio
from collections import OrderedDict

from ingest.monetdb.naming import THREAD_POOL
from ingest.streams.context import get_streams_context
from ingest.streams.streamexception import StreamException
from ingest.tsjson import jsonparser
from ingest.tsjson.jsonparser import parse_json_create_stream, parse_json_delete_stream, validate_json_tuples
from tshttp.tsjsonhandler import TSBaseJSONHandler


class StreamInfo(TSBaseJSONHandler):
    """RESTful API for stream's input"""

    def get(self, schema_name, stream_name):  # check a single stream data
        try:  # check if stream exists, if not return 404
            stream = get_streams_context().get_existing_stream(schema_name, stream_name)
        except StreamException as ex:
            self.write_error(404, **{'message': ex.__str__()})
            return

        self.write(OrderedDict(stream.get_data_dictionary()))
        self.set_status(200)


class JSONInput(TSBaseJSONHandler):
    """Add tuples to streams"""

    async def post(self):  # add data to a stream
        errors = []
        try:
            grouped_streams = jsonparser.parse_json_line(self.read_body())
            for key, values in grouped_streams.items():
                try:
                    metric = get_streams_context().get_existing_metric(key)
                    validate_json_tuples(metric, values)
                    await asyncio.wrap_future(THREAD_POOL.submit(metric.insert_values, values, 0))
                except StreamException as ex:
                    errors.append(ex.args[0]['message'])
        except StreamException as ex:
            errors.append(ex.args[0]['message'])

        if len(errors) > 0:
            self.write_error(400, **{'message': errors})
        else:
            self.set_status(201)


class StreamsHandling(TSBaseJSONHandler):
    """Admin class for creating/deleting streams"""

    def get(self):  # get all streams data
        results = get_streams_context().get_streams_data()
        self.write(results)
        self.set_status(200)

    async def post(self):
        try:
            json_schema = self.read_body()
            parse_json_create_stream(json_schema)
            stream_context = get_streams_context()
            await asyncio.wrap_future(THREAD_POOL.submit(stream_context.add_new_stream_with_json, json_schema))
        except StreamException as ex:
            self.write_error(400, **{'message': ex.args[0]['message']})
            return

        self.set_status(201)

    async def delete(self):
        try:
            json_schema = self.read_body()
            parse_json_delete_stream(json_schema)
            stream_context = get_streams_context()
            await asyncio.wrap_future(THREAD_POOL.submit(stream_context.delete_existing_stream, json_schema))
        except StreamException as ex:
            self.write_error(400, **{'message': ex.args[0]['message']})
            return

        self.set_status(204)
