from collections import OrderedDict
from typing import Dict, Any, List, Set

from ingest.monetdb.mapiconnection import PyMonetDBConnection
from ingest.monetdb.naming import get_context_entry_name, get_schema_and_stream_name
from ingest.streams.stream import BaseIOTStream
from ingest.streams.streamcreator import validate_json_schema_and_create_stream, load_streams_from_database, \
    create_stream_from_influxdb
from ingest.streams.streamexception import StreamException, CONTEXT_LOOKUP


class IOTStreams(object):
    """The Streams context held in the guardian"""

    def __init__(self, con_hostname: str, con_port: int, con_user: str, con_password: str, con_database: str):
        self._context = OrderedDict()  # dictionary of schema_name + METRIC_SEPARATOR + stream_name -> IOTStream
        # TODO we want a connection pool!!
        self._connection = PyMonetDBConnection(con_hostname, con_port, con_user, con_password, con_database)
        self._connection.init_timetrails()
        self.synchronize_stream_context()

    def close(self) -> None:
        self._connection.close()

    def get_existing_metric(self, concat_name: str) -> BaseIOTStream:
        if concat_name not in self._context:
            names = get_schema_and_stream_name(concat_name)
            error_message = "The stream %s in schema %s does not exist!" % (names[1], names[0])
            raise StreamException({'type': CONTEXT_LOOKUP, 'message': error_message})
        res = self._context[concat_name]
        return res

    def get_existing_stream(self, schema_name: str, stream_name: str) -> BaseIOTStream:
        concat_name = get_context_entry_name(schema_name, stream_name)
        return self.get_existing_metric(concat_name)

    def add_new_stream_with_json(self, validating_schema) -> None:
        schema_name = validating_schema['schema']
        stream_name = validating_schema['stream']
        concat_name = get_context_entry_name(schema_name, stream_name)

        if concat_name in self._context:
            error_message = "The stream %s in schema %s already exists!" % (stream_name, schema_name)
            raise StreamException({'type': CONTEXT_LOOKUP, 'message': error_message})

        new_stream = validate_json_schema_and_create_stream(self._connection, validating_schema)
        table_id = self._connection.create_stream(new_stream.get_schema_name(), new_stream.get_stream_name(),
                                                  new_stream.get_create_sql_column_statement(),
                                                  new_stream.get_stream_table_sql_statement())
        new_stream.set_table_id(table_id)  # set the table id!!
        new_stream.start_stream()
        self._context[concat_name] = new_stream

    def get_or_add_new_stream_with_influxdb(self, concat_name: str, tags: List[str], values: Dict[str, Any]):
        names = get_schema_and_stream_name(concat_name)
        if concat_name not in self._context:
            new_stream = create_stream_from_influxdb(self._connection, names[0], names[1], tags, values[0])
            table_id = self._connection.create_stream(new_stream.get_schema_name(), new_stream.get_stream_name(),
                                                      new_stream.get_create_sql_column_statement(),
                                                      new_stream.get_stream_table_sql_statement())
            new_stream.set_table_id(table_id)  # set the table id!!
            new_stream.start_stream()
            self._context[concat_name] = new_stream
        return self._context[concat_name]

    def force_flush_streams(self, streams: Set[str]) -> None:
        filtered = {conc_name: self._context[conc_name] for conc_name in self._context if conc_name in streams}
        for value in filtered.values():
            value.flush_data(forced=True)

    def delete_existing_stream(self, validating_schema) -> None:
        schema_name = validating_schema['schema']
        stream_name = validating_schema['stream']
        concat_name = get_context_entry_name(schema_name, stream_name)

        if concat_name not in self._context:
            error_message = "The stream %s in schema %s does not exist!" % (stream_name, schema_name)
            raise StreamException({'type': CONTEXT_LOOKUP, 'message': error_message})

        old_stream = self._context[concat_name]
        del self._context[concat_name]
        old_stream.stop_stream()
        self._connection.delete_stream(schema_name, stream_name, old_stream.get_table_id())

    def synchronize_stream_context(self) -> None:  # to synchronize with the database
        tables, columns = self._connection.get_database_streams()
        current_streams = list(self._context.keys())
        new_streams, removed_streams = load_streams_from_database(self._connection, current_streams, tables, columns)
        for key in removed_streams:
            value = self._context[key]
            del self._context[key]
            value.stop_stream()
        for value in new_streams.values():
            value.start_stream()
        self._context.update(new_streams)

    def get_streams_data(self) -> Dict[str, Any]:
        res = {'streams_count': len(self._context),
               'streams_listing': [OrderedDict(value.get_data_dictionary()) for value in self._context.values()]}
        return res

STREAMS_CONTEXT = None


def init_streams_context(con_hostname: str, con_port: int, con_user: str, con_password: str, con_database: str) -> None:
    global STREAMS_CONTEXT
    STREAMS_CONTEXT = IOTStreams(con_hostname, con_port, con_user, con_password, con_database)


def get_streams_context() -> IOTStreams:
    global STREAMS_CONTEXT
    return STREAMS_CONTEXT
