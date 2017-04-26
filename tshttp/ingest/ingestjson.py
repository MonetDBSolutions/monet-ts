import asyncio
import tornado.web, tornado.escape

from collections import OrderedDict
from jsonschema import ValidationError
from ingest.inputs.jsonschemas import CREATE_STREAMS_SCHEMA, DELETE_STREAMS_SCHEMA
from ingest.monetdb.naming import THREAD_POOL
from ingest.streams.context import get_streams_context


class JsonHandler(tornado.web.RequestHandler):
    """Request handler where requests and responses speak JSON."""

    def data_received(self, chunk):
        pass

    def set_default_headers(self):
        self.set_header('Content-Type', 'application/json')

    def write_error(self, status_code, **kwargs):
        if 'message' not in kwargs:
            if status_code == 405:
                kwargs['message'] = 'Invalid HTTP method.'
            else:
                kwargs['message'] = 'Unknown error.'

        self.write(tornado.escape.json_encode(kwargs))
        self.set_status(status_code)


class StreamInput(JsonHandler):
    """RESTful API for stream's input"""

    def get(self, schema_name, stream_name):  # check a single stream data
        try:  # check if stream exists, if not return 404
            stream = get_streams_context().get_existing_stream(schema_name, stream_name)
        except BaseException as ex:
            self.write_error(404, **{'message': ex.__str__()})
            return

        self.write(tornado.escape.json_encode(OrderedDict(stream.get_data_dictionary())))
        self.set_status(200)

    async def post(self, schema, stream):  # add data to a stream
        try:  # check if stream exists, if not return 404
            stream = get_streams_context().get_existing_stream(schema, stream)
        except BaseException as ex:
            self.write_error(404, **{'message': ex.__str__()})
            return

        try:  # validate and insert data, if not return 400
            await asyncio.wrap_future(THREAD_POOL.submit(stream.insert_json,
                                                         tornado.escape.json_decode(self.request.body)))
        except (ValidationError, BaseException) as ex:
            self.write_error(400, **{'message': ex.__str__()})
            return

        self.set_status(201)


class StreamsInfo(JsonHandler):
    """Collect all streams information"""

    def get(self):  # get all streams data
        results = get_streams_context().get_streams_data()
        self.write(tornado.escape.json_encode(results))
        self.set_status(200)


class StreamsHandling(JsonHandler):
    """Admin class for creating/deleting streams"""

    async def post(self):
        try:
            schema_to_validate = tornado.escape.json_decode(self.request.body)
            CREATE_STREAMS_SCHEMA.validate(schema_to_validate)
            stream_context = get_streams_context()
            await asyncio.wrap_future(THREAD_POOL.submit(stream_context.add_new_stream, schema_to_validate))
        except (ValidationError, BaseException) as ex:
            self.write_error(400, **{'message': ex.__str__()})
            return

        self.set_status(201)

    async def delete(self):
        try:
            schema_to_validate = tornado.escape.json_decode(self.request.body)
            DELETE_STREAMS_SCHEMA.validate(schema_to_validate)
        except BaseException as ex:
            self.write_error(400, **{'message': ex.__str__()})
            return

        try:  # check if stream exists, if not return 404
            stream_context = get_streams_context()
            await asyncio.wrap_future(THREAD_POOL.submit(stream_context.delete_existing_stream, schema_to_validate))
        except BaseException as ex:
            self.write_error(404, **{'message': ex.__str__()})
            return

        self.set_status(204)
