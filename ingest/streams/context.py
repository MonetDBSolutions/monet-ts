from collections import OrderedDict

from ingest.monetdb.mapiconnection import PyMonetDBConnection
from ingest.monetdb.naming import get_context_entry_name
from ingest.streams.streamcreator import validate_json_schema_and_create_stream, load_streams_from_database
from ingest.streams.streamexception import StreamException


class IOTStreams(object):
    """ The Streams context held in the guardian"""

    def __init__(self, con_hostname, con_port, con_user, con_password, con_database):
        # self._locker = RWLock()
        self._context = OrderedDict()  # dictionary of schema_name + '.' + stream_name -> IOTStream
        self._connection = PyMonetDBConnection(con_hostname, con_port, con_user, con_password, con_database)
        self.synchronize_stream_context()

    def close(self):
        self._connection.close()

    def add_new_stream(self, validating_schema):
        schema_name = validating_schema['schema']
        stream_name = validating_schema['stream']
        concat_name = get_context_entry_name(schema_name, stream_name)

        # self._locker.acquire_write()
        if concat_name in self._context:
            # self._locker.release()
            raise StreamException("The stream " + stream_name + " in schema " + stream_name + " already exists!")
        try:
            new_stream = validate_json_schema_and_create_stream(self._connection, validating_schema)
            table_id = self._connection.create_stream(new_stream.get_schema_name(), new_stream.get_stream_name(),
                                                      new_stream.get_create_sql_column_statement(),
                                                      new_stream.get_stream_table_sql_statement())
            new_stream.set_table_id(table_id) # set the table id!!
            new_stream.start_stream()
            self._context[concat_name] = new_stream
        except:
            # self._locker.release()
            raise
        # self._locker.release()

    def delete_existing_stream(self, validating_schema):
        schema_name = validating_schema['schema']
        stream_name = validating_schema['stream']
        concat_name = get_context_entry_name(schema_name, stream_name)

        # self._locker.acquire_write()
        if concat_name not in self._context:
            # self._locker.release()
            raise StreamException("The stream " + stream_name + " in schema " + stream_name + " does not exist!")
        try:
            old_stream = self._context[concat_name]
            del self._context[concat_name]
            old_stream.stop_stream()
            self._connection.delete_stream(schema_name, stream_name, old_stream.get_table_id())
        except:
            # self._locker.release()
            raise
        # self._locker.release()

    def synchronize_stream_context(self):  # to synchronize with the database
        # self._locker.acquire_write()
        try:
            tables, columns = self._connection.get_database_streams()
            current_streams = list(self._context.keys())
            new_streams, removed_streams = load_streams_from_database(self._connection, current_streams, tables,
                                                                      columns)
            for key in removed_streams:
                value = self._context[key]
                del self._context[key]
                value.stop_stream()
            for value in new_streams.values():
                value.start_stream()
            self._context.update(new_streams)
        except:
            # self._locker.release()
            raise
        # self._locker.release()

    def get_existing_stream(self, schema_name, stream_name):
        concat_name = get_context_entry_name(schema_name, stream_name)
        # self._locker.acquire_read()
        if concat_name not in self._context:
            # self._locker.release()
            raise StreamException("The stream " + stream_name + " in schema " + stream_name + " does not exist!")
        res = self._context[concat_name]
        # self._locker.release()
        return res

    def get_streams_data(self):
        # self._locker.acquire_read()
        res = {'streams_count': len(self._context),
               'streams_listing': [OrderedDict(value.get_data_dictionary()) for value in self._context.values()]}
        # self._locker.release()
        return res

STREAMS_CONTEXT = None


def init_streams_context(con_hostname, con_port, con_user, con_password, con_database):
    global STREAMS_CONTEXT
    STREAMS_CONTEXT = IOTStreams(con_hostname, con_port, con_user, con_password, con_database)


def get_streams_context():
    global STREAMS_CONTEXT
    return STREAMS_CONTEXT
