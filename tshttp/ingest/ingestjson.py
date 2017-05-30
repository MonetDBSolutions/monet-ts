from collections import OrderedDict

from ingest.streams.guardianexception import GuardianException
from ingest.tsjson.jsonparser import json_create_stream, json_delete_stream, add_json_lines, json_get_single_stream, \
    json_get_all_streams
from tshttp.tshandlers import TSJSONHandler


class StreamInfo(TSJSONHandler):
    """RESTful API for stream's input"""

    async def get(self, schema_name, stream_name):  # check a single stream data
        try:
            result = await json_get_single_stream(schema_name, stream_name)
            self.write(result)
            self.set_status(200)
        except GuardianException as ex:
            self.write_guardian_exception(ex)


class JSONInput(TSJSONHandler):
    """Add tuples to streams"""

    async def post(self):  # add data to a stream
        try:
            await add_json_lines(self.read_body())
            self.set_status(201)
        except GuardianException as ex:
            self.write_guardian_exception(ex)


class StreamsHandling(TSJSONHandler):
    """Create, delete and consult streams. Just pure CRUD"""

    async def get(self):  # get all streams data
        try:
            result = await json_get_all_streams()
            self.write(OrderedDict([('count', len(result)), ('listing', result)]))
            self.set_status(200)
        except GuardianException as ex:
            self.write_guardian_exception(ex)

    async def post(self):
        try:
            await json_create_stream(self.read_body())
            self.set_status(201)
        except GuardianException as ex:
            self.write_guardian_exception(ex)

    async def delete(self):
        try:
            await json_delete_stream(self.read_body())
            self.set_status(204)
        except GuardianException as ex:
            self.write_guardian_exception(ex)
