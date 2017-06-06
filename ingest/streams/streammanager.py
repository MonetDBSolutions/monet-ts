from typing import Dict, Any, List

from ingest.monetdb.naming import TIMESTAMP_COLUMN_NAME, INFLUXDB_TEXT_TYPE, INFLUXDB_BOOL_TYPE, INFLUXDB_INTEGER_TYPE,\
    INFLUXDB_FLOAT_TYPE
from ingest.streams.streamcache import get_stream_cache
from ingest.streams.guardianexception import GuardianException, JSON_SCHEMA_CREATE_VIOLATION, INFLUXDB_LINE_INSERT_VIOLATION
from ingest.tsjson.jsonschemas import TIMESTAMP_WITH_TIMEZONE_TYPE_EXTERNAL, TEXT_INPUT, BOOLEAN_INPUT,\
    BIGINTEGER_INPUT, DOUBLE_PRECISION_INPUT


def get_single_json_stream(schema_name: str, stream_name: str) -> Dict[str, Any]:
    return get_stream_cache().get_stream_details_by_schema(schema_name, stream_name)


def get_all_json_streams() -> List[Dict[str, Any]]:
    return get_stream_cache().get_all_streams_details()


def create_json_stream(schema: Dict[Any, Any]) -> None:
    found_columns = []
    errors = []

    for column in schema['columns']:
        next_name = column['name']

        if next_name in found_columns:
            errors.append("The column %s is duplicated?" % next_name)
            continue

        if next_name == TIMESTAMP_COLUMN_NAME:
            errors.append("The column name %s is reserved!" % TIMESTAMP_COLUMN_NAME)
            continue

        found_columns.append(next_name)
        column['isTag'] = False

    if 'tags' in schema:
        for tag in schema['tags']:
            if tag in found_columns:
                errors.append("The column %s is duplicated?" % tag)
                continue

            found_columns.append(tag)
            schema['columns'].append({'name': tag, 'type': TEXT_INPUT, 'nullable': False, 'isTag': True})

    if len(errors) > 0:
        raise GuardianException(where=JSON_SCHEMA_CREATE_VIOLATION, message=errors)

    schema['columns'].append({'name': TIMESTAMP_COLUMN_NAME, 'type': TIMESTAMP_WITH_TIMEZONE_TYPE_EXTERNAL,
                              'nullable': True, 'isTag': False})
    get_stream_cache().create_stream(schema['schema'], schema['stream'], schema['columns'])


def delete_json_stream(schema: Dict[Any, Any]) -> None:
    get_stream_cache().delete_stream(schema['schema'], schema['stream'])


INFLUXDB_SWITCHER = {INFLUXDB_BOOL_TYPE: BOOLEAN_INPUT, INFLUXDB_INTEGER_TYPE: BIGINTEGER_INPUT,
                     INFLUXDB_FLOAT_TYPE: DOUBLE_PRECISION_INPUT, INFLUXDB_TEXT_TYPE: TEXT_INPUT}


def create_stream_from_influxdb(metric: str, column: Dict[str, Dict[str, Any]]) -> None:
    validated_columns = []
    found_columns = []
    errors = []

    for key, value in column.items():
        if key in found_columns:
            errors.append("The column %s is duplicated?" % key)
            continue

        if key == TIMESTAMP_COLUMN_NAME:
            errors.append("The column name %s is reserved!" % TIMESTAMP_COLUMN_NAME)
            continue

        found_columns.append(key)

        if value['isTag']:
            validated_columns.append({'name': key, 'type': TEXT_INPUT, 'nullable': False, 'isTag': True})
        else:
            next_type = INFLUXDB_SWITCHER.get(value['type'], TEXT_INPUT)
            validated_columns.append({'name': key, 'type': next_type, 'nullable': True, 'isTag': False})

    validated_columns.append({'name': TIMESTAMP_COLUMN_NAME, 'type': TIMESTAMP_WITH_TIMEZONE_TYPE_EXTERNAL,
                              'nullable': True, 'isTag': False})
    if len(errors) > 0:
        raise GuardianException(where=INFLUXDB_LINE_INSERT_VIOLATION, message=errors)

    (schema, stream) = metric.split('.')
    get_stream_cache().create_stream(schema, stream, validated_columns)
