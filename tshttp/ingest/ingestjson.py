from ingest.monetdb.mapiconnection import get_mapi_connection
from ingest.streams.streamexception import StreamException
from ingest.tsjson.jsonparser import json_create_stream, json_delete_stream, add_json_lines
from tshttp.tsjsonhandler import TSBaseJSONHandler


class StreamInfo(TSBaseJSONHandler):
    """RESTful API for stream's input"""

    def get(self, schema_name, stream_name):  # check a single stream data
        try:
            self.write(get_mapi_connection().get_single_database_stream(schema_name, stream_name))
        except StreamException as ex:
            self.write_error(404, **{'message': ex.__str__()})
            return

        self.set_status(200)


class JSONInput(TSBaseJSONHandler):
    """Add tuples to streams"""

    async def post(self):  # add data to a stream
        errors = await add_json_lines(self.read_body())
        if len(errors) > 0:
            self.write_error(400, **{'messages': errors})
        else:
            self.set_status(201)


class StreamsHandling(TSBaseJSONHandler):
    """Admin class for creating/deleting streams"""

    def get(self):  # get all streams data
        try:
            self.write(get_mapi_connection().get_database_streams())
        except StreamException as ex:
            self.write_error(404, **{'message': ex.__str__()})
            return

        self.set_status(200)

    async def post(self):
        error = await json_create_stream(self.read_body())
        if error is not None:
            self.write_error(400, **{'message': error})
        else:
            self.set_status(201)

    async def delete(self):
        error = await json_delete_stream(self.read_body())
        if error is not None:
            self.write_error(400, **{'message': error})
        else:
            self.set_status(204)
