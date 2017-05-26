from typing import Dict, Any

from ingest.monetdb.mapiconnection import get_mapi_connection
from ingest.monetdb.naming import TIMESTAMP_COLUMN_NAME, INFLUXDB_TEXT_TYPE, INFLUXDB_BOOL_TYPE, INFLUXDB_INTEGER_TYPE,\
    INFLUXDB_FLOAT_TYPE
from ingest.streams.streamexception import StreamException, JSON_SCHEMA_CREATE_VIOLATION, INFLUXDB_LINE_INSERT_VIOLATION
from ingest.tsjson.jsonschemas import TIMESTAMP_WITH_TIMEZONE_TYPE_EXTERNAL, TEXT_INPUT, BOOLEAN_INPUT,\
    BIGINTEGER_INPUT, DOUBLE_PRECISION_INPUT


def _create_stream_sql(column_name: str, data_type: str, is_nullable: bool, limit: int=None) -> str:
    null_word = 'NOT NULL' if not is_nullable else 'NULL'
    data_type_real = data_type + "(" + str(limit) + ")" if limit is not None else data_type
    return "\"%s\" %s %s" % (column_name, data_type_real, null_word)


def create_json_stream(schema: Dict[Any, Any]) -> None:
    validated_columns = []
    found_columns = []
    errors = []

    for column in schema['columns']:  # create the data types dictionary
        next_name = column['name']

        if next_name in found_columns:
            errors.append("The column %s is duplicated?" % next_name)
            continue

        if next_name == TIMESTAMP_COLUMN_NAME:
            errors.append("The column name %s is reserved?" % TIMESTAMP_COLUMN_NAME)
            continue

        found_columns.extend(next_name)
        validated_columns.append(_create_stream_sql(column['name'], column['type'], column['nullable'],
                                                    column.get('nullable', None)))

    if 'tags' in schema:
        for tag in schema['tags']:
            if tag in found_columns:
                errors.append("The column %s is duplicated?" % tag)
                continue

            found_columns.extend(tag)
            validated_columns.append(_create_stream_sql(tag, TEXT_INPUT, False, None))

    if len(errors) > 0:
        raise StreamException({'type': JSON_SCHEMA_CREATE_VIOLATION, 'message': errors.__str__()})

    validated_columns.append(_create_stream_sql(TIMESTAMP_COLUMN_NAME, TIMESTAMP_WITH_TIMEZONE_TYPE_EXTERNAL, True, None))
    get_mapi_connection().create_stream(schema['schema'], schema['stream'], ','.join(validated_columns))


def delete_json_stream(schema: Dict[Any, Any]) -> None:
    get_mapi_connection().delete_stream(schema['schema'], schema['stream'])


INFLUXDB_SWITCHER = {INFLUXDB_BOOL_TYPE: BOOLEAN_INPUT, INFLUXDB_INTEGER_TYPE: BIGINTEGER_INPUT,
                     INFLUXDB_FLOAT_TYPE: DOUBLE_PRECISION_INPUT, INFLUXDB_TEXT_TYPE: TEXT_INPUT}


def create_stream_from_influxdb(metric: str, column: Dict[str, Dict[str, Any]]) -> None:
    validated_columns = []
    found_columns = []
    errors = []

    for key, value in column.items():  # create the data types dictionary
        if key in validated_columns:
            errors.append("The column %s is duplicated?" % key)
            continue

        if key == TIMESTAMP_COLUMN_NAME:
            errors.append("The column name %s is reserved?" % TIMESTAMP_COLUMN_NAME)
            continue

        found_columns.append(key)

        if value['isTag']:
            validated_columns.append(_create_stream_sql(key, TEXT_INPUT, False, None))
        else:
            next_type = INFLUXDB_SWITCHER.get(value['isTag'], TEXT_INPUT)
            validated_columns.append(_create_stream_sql(key, next_type, True, None))

    validated_columns.append(_create_stream_sql(TIMESTAMP_COLUMN_NAME, TIMESTAMP_WITH_TIMEZONE_TYPE_EXTERNAL,
                                                True, None))

    if len(errors) > 0:
        raise StreamException({'type': INFLUXDB_LINE_INSERT_VIOLATION, 'message': errors.__str__()})

    (schema, stream) = metric.split('.')

    get_mapi_connection().create_stream(schema, stream, ','.join(validated_columns))
