import sched
import time

from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from typing import Dict, Any
from jsonschema import Draft4Validator

from ingest.monetdb.mapiconnection import PyMonetDBConnection
from ingest.monetdb.naming import TIME_BASED_STREAM, TUPLE_BASED_STREAM, AUTO_BASED_STREAM, TIMESTAMP_COLUMN_NAME
from ingest.streams.datatypes import StreamDataType
from ingest.streams.streamexception import StreamException, TABLE_CONSTRAINTS_VALIDATION

SCHEDULER = sched.scheduler(time.time, time.sleep)


class BaseIOTStream(object):
    """Representation of a stream for validation"""
    __metaclass__ = ABCMeta

    def __init__(self, schema_name: str, stream_name: str, columns: Dict[str, StreamDataType],
                 json_validation_schema: Draft4Validator, connection: PyMonetDBConnection, table_id: str=""):
        self._schema_name = schema_name  # name of the schema
        self._stream_name = stream_name  # name of the stream
        self._columns = columns  # dictionary of name -> data_types
        self._validation_schema = json_validation_schema  # json validation schema for the inserts
        self._table_id = table_id  # for delete statement on table iot.webserverstreams
        self._inserts = []
        self._connection = connection

    def get_schema_name(self) -> str:
        return self._schema_name

    def get_stream_name(self) -> str:
        return self._stream_name

    def get_validation_schema(self) -> Draft4Validator:
        return self._validation_schema

    def get_create_sql_column_statement(self) -> str:  # for CREATE STREAM TABLE statement
        return ','.join([column.create_stream_sql() for column in self._columns.values()])

    def get_primary_keys_statement(self) -> str:  # for CREATE STREAM TABLE statement
        return ','.join([x.column_name() for x in self._columns.values() if x.is_tag()])

    @abstractmethod
    def get_stream_table_sql_statement(self) -> str:  # insert for timetrails.webserverflushing table
        pass

    def get_table_id(self) -> str:  # for the delete statement
        return self._table_id

    def set_table_id(self, table_id: str) -> None:
        self._table_id = table_id

    @abstractmethod
    def start_stream(self) -> None:
        pass

    @abstractmethod
    def stop_stream(self) -> None:
        pass

    @abstractmethod
    def get_flushing_dictionary(self) -> Dict[str, Any]:  # for information about the stream
        pass

    def get_data_dictionary(self):
        return {'columns': [OrderedDict((value.to_json_representation())) for value in self._columns.values()],
                'flushing': self.get_flushing_dictionary()}

    @abstractmethod
    def flush_data(self, forced=False):
        pass

    def insert_values(self, new_data, base_tuple_counter: int) -> None:
        errors = []
        column_names = self._columns.keys()
        parsed_array = []
        new_inserts = []
        time_to_input = int(round(time.time() * 1000))  # if the timestamp is missing

        for entry in new_data:
            base_tuple_counter += 1
            del parsed_array[:]

            if TIMESTAMP_COLUMN_NAME not in entry:
                entry[TIMESTAMP_COLUMN_NAME] = time_to_input

            for column in column_names:
                data_type = self._columns[column]  # get the corresponding data type
                if column not in entry.keys():  # check if the column is present or not
                    if data_type.is_nullable():
                        parsed_array.append("NULL")
                    else:
                        errors.append("The non nullable column %s is missing at the line %d!" %
                                      (column, base_tuple_counter))
                else:
                    parsed_array.append(data_type.convert_value_into_sql(entry[column]))

            new_inserts.append("(" + ','.join(parsed_array) + ")")

        if errors:
            raise StreamException({'type': TABLE_CONSTRAINTS_VALIDATION, 'message': errors})

        self._inserts.extend(new_inserts)
        self.flush_data()


class TupleBasedStream(BaseIOTStream):
    """Stream with tuple based flushing"""

    def __init__(self, schema_name, stream_name, columns, json_validation_schema, connection, table_id, interval):
        super().__init__(schema_name, stream_name, columns, json_validation_schema, connection, table_id)
        self._interval = interval

    def start_stream(self) -> None:
        pass

    def stop_stream(self) -> None:
        pass

    def get_flushing_dictionary(self) -> Dict[str, Any]:
        return {'base': 'tuple', 'interval': self._interval, 'tuples_inserted_per_basket': len(self._inserts)}

    def get_stream_table_sql_statement(self) -> str:
        return str(TUPLE_BASED_STREAM) + ',' + str(self._interval) + ",NULL"

    def flush_data(self, forced=False) -> None:
        if forced or len(self._inserts) > self._interval:
            sql_columns = ','.join(self._inserts[:self._interval])
            self._connection.insert_points(self._schema_name, self._stream_name, sql_columns)
            del self._inserts[:self._interval]


def time_based_flush(time_based_stream) -> None:
    time_based_stream.flush_data(forced=True)
    time_based_stream.upcoming_event = SCHEDULER.enter(time_based_stream.calc_time, 1, time_based_flush,
                                                       (time_based_stream,))


class TimeBasedStream(BaseIOTStream):
    """Stream with time based flushing"""

    def __init__(self, schema_name, stream_name, columns, json_validation_schema, connection, table_id, interval,
                 time_unit):
        super().__init__(schema_name, stream_name, columns, json_validation_schema, connection, table_id)
        self._time_unit = time_unit
        self._interval = interval
        if time_unit == 1:
            calc_time = interval
        elif time_unit == 2:
            calc_time = interval * 60
        else:
            calc_time = interval * 3600

        self.calc_time = calc_time
        self.upcoming_event = None

    def start_stream(self) -> None:
        self.upcoming_event = SCHEDULER.enter(self.calc_time, 1, time_based_flush, (self,))

    def stop_stream(self) -> None:
        SCHEDULER.cancel(self.upcoming_event)

    def get_flushing_dictionary(self) -> Dict[str, Any]:
        return {'base': 'time', 'interval': self._interval, 'time_unit': self._time_unit,
                'tuples_inserted_per_basket': len(self._inserts)}

    def get_stream_table_sql_statement(self) -> str:
        return str(TIME_BASED_STREAM) + ',' + str(self._interval) + ',' + str(self._time_unit)

    def flush_data(self, forced=False) -> None:
        if forced and len(self._inserts) > 0:
            sql_columns = ','.join(self._inserts)
            self._connection.insert_points(self._schema_name, self._stream_name, sql_columns)
            del self._inserts[:]


class AutoFlushedStream(BaseIOTStream):
    """Stream with flush every time a new batch is inserted"""

    def __init__(self, schema_name, stream_name, columns, json_validation_schema, connection, table_id):
        super().__init__(schema_name, stream_name, columns, json_validation_schema, connection, table_id)

    def start_stream(self) -> None:
        pass

    def stop_stream(self) -> None:
        pass

    def get_flushing_dictionary(self) -> Dict[str, Any]:
        return {'base': 'auto'}

    def get_stream_table_sql_statement(self) -> str:
        return str(AUTO_BASED_STREAM) + ',NULL,NULL'

    def flush_data(self, forced=False) -> None:
        sql_columns = ','.join(self._inserts)
        self._connection.insert_points(self._schema_name, self._stream_name, sql_columns)
        del self._inserts[:]
