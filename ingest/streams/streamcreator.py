from collections import OrderedDict, defaultdict
from jsonschema import Draft4Validator, FormatChecker

from ingest.inputs.jsonschemas import UNBOUNDED_TEXT_INPUTS, BOUNDED_TEXT_INPUTS, INTEGER_INPUTS, DATE_INPUTS, \
    BOOLEAN_INPUTS, FLOATING_POINT_PRECISION_INPUTS, TIME_INPUTS, TIMESTAMP_INPUTS, TIMED_FLUSH_IDENTIFIER, \
    TUPLE_FLUSH_IDENTIFIER, TIME_WITH_TIMEZONE_TYPE_INTERNAL, TIMESTAMP_WITH_TIMEZONE_TYPE_INTERNAL, \
    TIME_WITH_TIMEZONE_TYPE_EXTERNAL, TIMESTAMP_WITH_TIMEZONE_TYPE_EXTERNAL, AUTO_FLUSH_IDENTIFIER, \
    INTERVAL_TYPES_INTERNAL
from ingest.monetdb.naming import get_context_entry_name, TUPLE_BASED_STREAM, TIME_BASED_STREAM, AUTO_BASED_STREAM, \
    TIMESTAMP_COLUMN_NAME
from ingest.streams.datatypes import TextType, LimitedTextType, IntegerType, FloatType, DateType, TimeType, \
    TimestampType, BooleanType
from ingest.streams.stream import TupleBasedStream, TimeBasedStream, AutoFlushedStream
from ingest.streams.streamexception import StreamException

SWITCHER = [{'types': UNBOUNDED_TEXT_INPUTS, 'class': TextType},
            {'types': BOUNDED_TEXT_INPUTS, 'class': LimitedTextType},
            {'types': BOOLEAN_INPUTS, 'class': BooleanType},
            {'types': INTEGER_INPUTS + INTERVAL_TYPES_INTERNAL, 'class': IntegerType},
            {'types': FLOATING_POINT_PRECISION_INPUTS, 'class': FloatType},
            {'types': DATE_INPUTS, 'class': DateType},
            {'types': TIME_INPUTS, 'class': TimeType},
            {'types': TIMESTAMP_INPUTS, 'class': TimestampType}]

INTERVALS_DICTIONARY = {1: "interval year", 2: "interval year to month", 3: "interval month", 4: "interval day",
                        5: "interval day to hour", 6: "interval day to minute", 7: "interval day to second",
                        8: "interval hour", 9: "interval hour to minute", 10: "interval hour to second",
                        11: "interval minute", 12: "interval minute to second", 13: "interval second"}

TIME_FLUSH_DICTIONARY = {'s': 1, 'm': 2, 'h': 3}


def validate_json_schema_and_create_stream(connection, schema):
    validated_columns = OrderedDict()  # dictionary of name -> data_types
    errors = {}

    for column in schema['columns']:  # create the data types dictionary
        next_type = column['type']
        next_name = column['name']

        if next_name in validated_columns:
            errors[next_name] = 'The column ' + next_name + ' is duplicated!'
            continue

        if next_name == TIMESTAMP_COLUMN_NAME:
            errors[next_name] = 'The column name ' + TIMESTAMP_COLUMN_NAME + ' is reserved!'
            continue

        for entry in SWITCHER:  # allocate the proper type wrapper
            if next_type in entry['types']:
                try:
                    validated_columns[next_name] = entry['class'](**column)  # pass the json entry as kwargs
                except BaseException as ex:
                    errors[next_name] = ex.__str__()
                break

    # Add the timestamp column!
    validated_columns[TIMESTAMP_COLUMN_NAME] = TimestampType(**{'name': TIMESTAMP_COLUMN_NAME, 'nullable': True,
                                                                'type': TIMESTAMP_WITH_TIMEZONE_TYPE_EXTERNAL})

    if errors:
        raise StreamException(str(errors))  # dictionary of name -> error message

    properties = OrderedDict()
    req_fields = []

    for key, value in validated_columns.items():
        value.add_json_schema_entry(properties)  # append new properties entry
        if not value.is_nullable():  # check if it's required or not
            req_fields.append(key)

    json_schema = Draft4Validator({
        "title": "JSON schema to validate inserts in stream " + schema['schema'] + "." + schema['stream'],
        "description": "Validate the inserted properties", "$schema": "http://json-schema.org/draft-04/schema#",
        "id": "http://monetdb.com/schemas/" + schema['schema'] + "." + schema['stream'] + ".json", "type": "array",
        "minItems": 1, "items": {"type": "object", "properties": properties, "required": req_fields,
                                 "additionalProperties": False}
    }, format_checker=FormatChecker())

    flushing_object = schema['flushing']  # check the flush method

    if flushing_object['base'] == TIMED_FLUSH_IDENTIFIER:
        res = TimeBasedStream(schema_name=schema['schema'], stream_name=schema['stream'], columns=validated_columns,
                              json_validation_schema=json_schema, connection=connection, table_id="",
                              interval=flushing_object['interval'],
                              time_unit=TIME_FLUSH_DICTIONARY[flushing_object['unit']])
    elif flushing_object['base'] == TUPLE_FLUSH_IDENTIFIER:
        res = TupleBasedStream(schema_name=schema['schema'], stream_name=schema['stream'], columns=validated_columns,
                               json_validation_schema=json_schema, connection=connection, table_id="",
                               interval=flushing_object['interval'])
    elif flushing_object['base'] == AUTO_FLUSH_IDENTIFIER:
        res = AutoFlushedStream(schema_name=schema['schema'], stream_name=schema['stream'], columns=validated_columns,
                                json_validation_schema=json_schema, connection=connection, table_id="")
    else:
        raise StreamException({"stream": "Invalid stream type!"})
    return res


def load_streams_from_database(connection, current_streams, tables, columns):
    # FLUSHING_STREAMS = {1: 'TupleBasedStream', 2: 'TimeBasedStream', 3: 'AutoFlushedStream'}
    # for tables: [0] -> id, [1] -> schema, [2] -> name, [3] -> base, [4] -> interval, [5] -> unit
    # for columns: [0] -> id, [1] -> table_id, [2] -> name, [3] -> type, [4] -> type_digits, [5] -> type_scale,
    # [6] -> is_null

    new_streams = {}
    found_streams = []
    errors = None

    grouped_columns = defaultdict(list)  # group the columns to the respective tables
    for entry in columns:
        grouped_columns[entry[1]].append(entry)

    for entry in tables:
        next_concatenated_name = get_context_entry_name(entry[1], entry[2])
        retrieved_columns = grouped_columns[entry[0]]

        if next_concatenated_name not in current_streams:
            built_columns = OrderedDict()  # dictionary of name -> data_types
            has_timestamp = False

            for column in retrieved_columns:
                kwargs_dic = {'name': column[2], 'nullable': column[6]}
                if TIMESTAMP_COLUMN_NAME == kwargs_dic['name']:
                    has_timestamp = True
                next_switch = column[3]
                kwargs_dic['type'] = next_switch
                if next_switch in BOUNDED_TEXT_INPUTS:
                    kwargs_dic['limit'] = column[4]
                elif next_switch in INTERVAL_TYPES_INTERNAL:
                    kwargs_dic['type'] = INTERVALS_DICTIONARY.get(column[4])
                elif next_switch == TIME_WITH_TIMEZONE_TYPE_INTERNAL:
                    kwargs_dic['type'] = TIME_WITH_TIMEZONE_TYPE_EXTERNAL
                elif next_switch == TIMESTAMP_WITH_TIMEZONE_TYPE_INTERNAL:
                    kwargs_dic['type'] = TIMESTAMP_WITH_TIMEZONE_TYPE_EXTERNAL

                valid_type = False
                for variable in SWITCHER:  # allocate the proper type wrapper
                    if kwargs_dic['type'] in variable['types']:
                        built_columns[kwargs_dic['name']] = variable['class'](**kwargs_dic)
                        valid_type = True
                        break
                if not valid_type:
                    errors[next_concatenated_name] = "Not a valid type: " + kwargs_dic['type']
                    break

            if errors is not None and next_concatenated_name in errors:
                continue
            elif has_timestamp:
                properties = OrderedDict()
                req_fields = []

                for key, value in built_columns.items():
                    value.add_json_schema_entry(properties)  # append new properties entry
                    if not value.is_nullable():  # check if it's required
                        req_fields.append(key)

                json_schema = Draft4Validator({
                    "title": "JSON schema to validate inserts in stream " + entry[1] + "." + entry[2],
                    "description": "Validate properties", "$schema": "http://json-schema.org/draft-04/schema#",
                    "id": "http://monetdb.com/schemas/" + entry[1] + "." + entry[2] + ".json", "type": "array",
                    "minItems": 1, "items": {"type": "object", "properties": properties, "required": req_fields,
                                             "additionalProperties": False}
                }, format_checker=FormatChecker())

                if entry[3] == TUPLE_BASED_STREAM:
                    new_stream = TupleBasedStream(schema_name=entry[1], stream_name=entry[2], columns=built_columns,
                                                  json_validation_schema=json_schema, table_id=str(entry[0]),
                                                  interval=int(entry[4]), connection=connection)
                elif entry[3] == TIME_BASED_STREAM:
                    new_stream = TimeBasedStream(schema_name=entry[1], stream_name=entry[2], columns=built_columns,
                                                 json_validation_schema=json_schema, table_id=str(entry[0]),
                                                 interval=int(entry[4]), time_unit=entry[5], connection=connection)
                elif entry[3] == AUTO_BASED_STREAM:
                    new_stream = AutoFlushedStream(schema_name=entry[1], stream_name=entry[2], columns=built_columns,
                                                   json_validation_schema=json_schema, table_id=str(entry[0]),
                                                   connection=connection)
                else:
                    errors[next_concatenated_name] = "Invalid stream type!"
                    continue
                new_streams[next_concatenated_name] = new_stream
                found_streams.append(next_concatenated_name)

    if errors:
        raise StreamException(str(errors))  # dictionary of name -> error message

    removed_streams = list(filter(lambda x: x not in found_streams, current_streams))
    return new_streams, removed_streams
