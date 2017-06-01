from typing import Dict, Any, List

import datetime

from ingest.monetdb.naming import get_default_timestamp_value, my_monet_escape
from ingest.streams.streamcache import get_stream_cache
from ingest.streams.guardianexception import GuardianException, JSON_SCHEMA_INSERT_VIOLATION
from ingest.tsjson.jsonschemas import UNBOUNDED_TEXT_INPUTS, BOUNDED_TEXT_INPUTS, DATE_INPUTS, TIME_INPUTS, \
    TIMESTAMP_INPUTS, BOOLEAN_INPUTS, INTEGER_INPUTS, FLOATING_POINT_PRECISION_INPUTS


def str_converter(value) -> str:
    return "'" + my_monet_escape(value) + "'"


def bool_converter(value) -> str:
    return "'1'" if value is True else "'0'"


def number_converter(value) -> str:
    return "'" + str(value) + "'"  # there is no need to escape here...


def timestamp_converter(value) -> str:
    if type(value) != str:
        value = datetime.datetime.fromtimestamp(value).isoformat()
    return "'" + value + "'"


# for char and varchar check the limit
def extra_validation_bounded(stream_column, stream_entry_counter, insert_counter, value) -> str:
    correct_limit = stream_column['limit']
    next_length = len(value)

    if next_length > correct_limit:
        return 'Length exceeded for the column %s: %d > %d (line %d, %d)!' % (stream_column['name'], next_length,
                                                                              correct_limit, stream_entry_counter,
                                                                              insert_counter)
    else:
        return ''

TYPE_CHECK_DICT = {}

for entry in UNBOUNDED_TEXT_INPUTS + DATE_INPUTS + TIME_INPUTS:
    TYPE_CHECK_DICT[entry] = {'types': [str], 'extra': None, 'converter': str_converter}

for entry in BOUNDED_TEXT_INPUTS:
    TYPE_CHECK_DICT[entry] = {'types': [str], 'extra': extra_validation_bounded, 'converter': str_converter}

for entry in BOOLEAN_INPUTS:
    TYPE_CHECK_DICT[entry] = {'types': [bool], 'extra': None, 'converter': bool_converter}

for entry in INTEGER_INPUTS:
    TYPE_CHECK_DICT[entry] = {'types': [int], 'extra': None, 'converter': number_converter}

for entry in FLOATING_POINT_PRECISION_INPUTS:
    TYPE_CHECK_DICT[entry] = {'types': [float], 'extra': None, 'converter': number_converter}

for entry in TIMESTAMP_INPUTS:
    TYPE_CHECK_DICT[entry] = {'types': [str, int], 'extra': None, 'converter': timestamp_converter}


def insert_json_values(input_json: List[Dict[str, Any]]) -> None:
    time_to_input = "'" + get_default_timestamp_value() + "'"  # if the timestamp is missing
    mapi_context = get_stream_cache()
    errors = []
    stream_entry_counter = 0

    for stream_entry in input_json:
        stream_entry_counter += 1

        # check if the stream exists
        next_columns = mapi_context.try_get_stream(stream_entry['schema'], stream_entry['stream'])
        if next_columns is None:
            errors.append('The stream %s.%s at line %d does not exist!' %
                          (stream_entry['schema'], stream_entry['stream'], stream_entry_counter))
            continue

        next_batch = []

        for entry in stream_entry['values']:
            insert_counter = 0
            next_inserts = ['NULL'] * len(next_columns)

            for key, value in entry.items():
                insert_counter += 1

                # check if the column exists in the stream
                found_column = None
                index = 0
                for single_column in next_columns:
                    if single_column['name'] == key:
                        found_column = single_column
                        break
                    else:
                        index += 1

                if found_column is None:
                    errors.append('The column %s does not exist in the stream %s.%s (line %d, %d)!' %
                                  (key, stream_entry['schema'], stream_entry['stream'], stream_entry_counter,
                                   insert_counter))
                    continue

                # check if the type is correct
                next_type_validation = TYPE_CHECK_DICT[found_column['type']]
                next_correct_types = next_type_validation['types']
                next_type = type(value)
                if next_type not in next_correct_types:
                    errors.append('The value for the column %s is wrong: %s not in [%s] (line %d, %d)!' %
                                  (found_column['name'], next_type.__name__,
                                   ','.join(map(lambda x: x.__name__, next_correct_types)), stream_entry_counter,
                                   insert_counter))
                    continue

                # extra validations
                next_extra_validation = next_type_validation['extra']
                if next_extra_validation is not None:
                    next_error = next_extra_validation(found_column, stream_entry_counter, insert_counter, value)
                    if next_error != '':
                        errors.append(next_error)
                        continue

                next_inserts[index] = next_type_validation['converter'](value)

                insert_counter += 1

            # add the timestamp column if missing
            if next_inserts[-1] == 'NULL':
                next_inserts[-1] = time_to_input

            next_batch.append("(" + ",".join(next_inserts) + ")")

        if len(next_batch) > 0:
            mapi_context.insert_into_stream(stream_entry['schema'], stream_entry['stream'], ','.join(next_batch))

    if len(errors):
        raise GuardianException(where=JSON_SCHEMA_INSERT_VIOLATION, message=errors)
