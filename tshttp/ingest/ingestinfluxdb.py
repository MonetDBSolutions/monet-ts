import asyncio

from ingest.monetdb.naming import THREAD_POOL
from ingest.streams.context import get_streams_context
from ingest.streams.streamexception import StreamException
from ingest.tsinfluxline import influxdblineparser
from ingest.tsinfluxline.linereader import read_chunk_lines, CHUNK_SIZE
from tshttp.tslinehandler import TSBaseLineHandler


class InfluxDBInput(TSBaseLineHandler):
    """Ingest lines under the InfluxDB protocol"""

    async def post(self):
        base_tuple_counter = 0
        errors = []

        for lines in read_chunk_lines(self.request.body.decode("utf-8"), CHUNK_SIZE):
            try:
                grouped_streams = influxdblineparser.parse_influxdb_line(lines, base_tuple_counter)
                for key, values in grouped_streams.items():
                    try:  # TODO check UTF-8 strings?
                        stream = get_streams_context().get_existing_metric(key)
                        await asyncio.wrap_future(THREAD_POOL.submit(stream.insert_values, values['values'],
                                                                     base_tuple_counter))
                    except StreamException as ex:
                        errors.append(ex.args[0]['message'])
            except StreamException as ex:
                errors.append(ex.args[0]['message'])

            base_tuple_counter += CHUNK_SIZE

        if len(errors) > 0:
            self.write_error(400, **{'messages': errors})
        else:
            self.set_status(201)


class InfluxDBDiscovery(TSBaseLineHandler):
    """Ingest a lines under the InfluxDB protocol or create the streams if they don't exist"""

    async def post(self):
        base_tuple_counter = 0
        errors = []

        for lines in read_chunk_lines(self.request.body.decode("utf-8"), CHUNK_SIZE):
            try:
                grouped_streams = influxdblineparser.parse_influxdb_line(lines, base_tuple_counter)
                for key, values in grouped_streams.items():
                    try:  # TODO check UTF-8 strings?
                        stream = get_streams_context().get_or_add_new_stream_with_influxdb(key, values['tags'],
                                                                                           values['values'])
                        await asyncio.wrap_future(THREAD_POOL.submit(stream.insert_values, values['values'],
                                                                     base_tuple_counter))
                    except StreamException as ex:
                        errors.append(ex.args[0]['message'])
            except StreamException as ex:
                errors.append(ex.args[0]['message'])

            base_tuple_counter += CHUNK_SIZE

        if len(errors) > 0:
            self.write_error(400, **{'messages': errors})
        else:
            self.set_status(201)
