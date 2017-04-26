import datetime
import sched
import time

from abc import ABCMeta, abstractmethod
from collections import OrderedDict

from ingest.monetdb.naming import TIME_BASED_STREAM, TUPLE_BASED_STREAM, AUTO_BASED_STREAM, TIMESTAMP_COLUMN_NAME
from ingest.streams.streamexception import StreamException

SCHEDULER = sched.scheduler(time.time, time.sleep)


class BaseIOTStream(object):
    """Representation of a stream for validation"""
    __metaclass__ = ABCMeta

    def __init__(self, schema_name, stream_name, columns, json_validation_schema, connection, table_id=""):
        self._schema_name = schema_name  # name of the schema
        self._stream_name = stream_name  # name of the stream
        self._columns = columns  # dictionary of name -> data_types
        self._validation_schema = json_validation_schema  # json validation schema for the inserts
        self._table_id = table_id  # for delete statement on table iot.webserverstreams
        self.inserts = []
        self.connection = connection

    def get_schema_name(self):
        return self._schema_name

    def get_stream_name(self):
        return self._stream_name

    def get_create_sql_column_statement(self):  # for CREATE STREAM TABLE statement
        base_sql = ','.join([column.create_stream_sql() for column in self._columns.values()])
        return base_sql

    @abstractmethod
    def get_stream_table_sql_statement(self):  # insert for iot.webserverflushing table
        pass

    def get_table_id(self):  # for the delete statement
        return self._table_id

    def set_table_id(self, table_id):
        self._table_id = table_id

    @abstractmethod
    def start_stream(self):
        pass

    @abstractmethod
    def stop_stream(self):
        pass

    @abstractmethod
    def get_flushing_dictionary(self):  # for information about the stream
        pass

    def get_data_dictionary(self):
        return {'schema': self._schema_name, 'stream': self._stream_name,
                'columns': [OrderedDict((value.to_json_representation())) for value in self._columns.values()],
                'flushing': self.get_flushing_dictionary()}

    def insert_json(self, new_data):
        self._validation_schema.validate(new_data)  # validate the stream's schema first
        self._insert_data(new_data)

    def _insert_data(self, new_data):
        errors = {}  # dictionary of column_name -> array of errors
        column_names = self._columns.keys()
        tuple_counter = 0
        parsed_array = []
        new_inserts = []
        time_to_input = datetime.datetime.utcnow().isoformat()

        for entry in new_data:
            tuple_counter += 1
            del parsed_array[:]

            if TIMESTAMP_COLUMN_NAME not in entry:
                entry[TIMESTAMP_COLUMN_NAME] = time_to_input

            for column in column_names:
                data_type = self._columns[column]  # get the corresponding data type
                if column not in entry.keys():  # check if the column is present or not
                    if data_type.is_nullable():
                        parsed_array.append("NULL")
                    else:
                        errors[column] = "Problem while parsing this column in tuple: " + tuple_counter
                else:
                    parsed_array.append("'" + data_type.convert_value_into_sql(entry[column]) + "'")

            new_inserts.append("(" + ','.join(parsed_array) + ")")

        if errors:
            raise StreamException(str(errors))  # dictionary of name -> error message

        self.inserts.extend(new_inserts)


class TupleBasedStream(BaseIOTStream):
    """Stream with tuple based flushing"""

    def __init__(self, schema_name, stream_name, columns, json_validation_schema, connection, table_id, interval):
        super(TupleBasedStream, self).__init__(schema_name, stream_name, columns, json_validation_schema, connection,
                                               table_id)
        self._interval = interval

    def start_stream(self):
        pass

    def stop_stream(self):
        pass

    def get_flushing_dictionary(self):
        return {'base': 'tuple', 'interval': self._interval, 'tuples_inserted_per_basket': len(self.inserts)}

    def get_stream_table_sql_statement(self):  # insert for iot.webserverflushing table
        return str(TUPLE_BASED_STREAM) + ',' + str(self._interval) + ",NULL"

    def _insert_data(self, new_data):
        super(TupleBasedStream, self)._insert_data(new_data)
        if len(self.inserts) > self._interval:
            sql_columns = ','.join(self.inserts[:self._interval])
            self.connection.insert_points(self._schema_name, self._stream_name, sql_columns)
            del self.inserts[:self._interval]


def time_based_flush(time_based_stream):
    if len(time_based_stream.inserts) > 0:  # flush only when there are tuples in the baskets
        sql_columns = ','.join(time_based_stream.inserts)
        time_based_stream.connection.insert_points(time_based_stream.get_schema_name(),
                                                   time_based_stream.get_stream_name(), sql_columns)
        del time_based_stream.inserts[:]
    time_based_stream.upcoming_event = SCHEDULER.enter(time_based_stream.calc_time, 1, time_based_flush,
                                                       (time_based_stream,))


class TimeBasedStream(BaseIOTStream):
    """Stream with time based flushing"""

    def __init__(self, schema_name, stream_name, columns, json_validation_schema, connection, table_id, interval,
                 time_unit):
        super(TimeBasedStream, self).__init__(schema_name, stream_name, columns, json_validation_schema, connection,
                                              table_id)
        self._time_unit = time_unit
        self._interval = interval
        if time_unit == 1:
            calc_time = interval
        elif time_unit == 2:
            calc_time = interval * 60
        else:
            calc_time = interval * 3600

        self.calc_time = calc_time

    def start_stream(self):
        self.upcoming_event = SCHEDULER.enter(self.calc_time, 1, time_based_flush, (self,))

    def stop_stream(self):
        SCHEDULER.cancel(self.upcoming_event)

    def get_flushing_dictionary(self):
        return {'base': 'time', 'interval': self._interval, 'time_unit': self._time_unit,
                'tuples_inserted_per_basket': len(self.inserts)}

    def get_stream_table_sql_statement(self):  # insert for iot.webserverflushing table
        return str(TIME_BASED_STREAM) + ',' + str(self._interval) + ',' + str(self._time_unit)


class AutoFlushedStream(BaseIOTStream):
    """Stream with flush every time a new batch is inserted"""

    def __init__(self, schema_name, stream_name, columns, json_validation_schema, connection, table_id):
        super(AutoFlushedStream, self).__init__(schema_name, stream_name, columns, json_validation_schema, connection,
                                                table_id)

    def start_stream(self):
        pass

    def stop_stream(self):
        pass

    def get_flushing_dictionary(self):
        return {'base': 'auto'}

    def get_stream_table_sql_statement(self):  # insert for iot.webserverflushing table
        return str(AUTO_BASED_STREAM) + ',NULL,NULL'

    def _insert_data(self, new_data):
        super(AutoFlushedStream, self)._insert_data(new_data)
        sql_columns = ','.join(self.inserts)
        self.connection.insert_points(self._schema_name, self._stream_name, sql_columns)
        del self.inserts[:]
