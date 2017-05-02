from collections import defaultdict
from typing import Any, Dict, List

from jsonschema import ValidationError

from ingest.inputs.jsonschemas import INSERT_DATA_SCHEMA, CREATE_STREAMS_SCHEMA, DELETE_STREAMS_SCHEMA
from ingest.monetdb.naming import get_context_entry_name
from ingest.streams.stream import BaseIOTStream
from ingest.streams.streamexception import StreamException, JSON_SCHEMA_CREATE_VIOLATION, \
    JSON_SCHEMA_LINE_SPLIT_VIOLATION, JSON_SCHEMA_INSERT_VIOLATION


def parse_json_create_stream(schema_to_validate: Any) -> None:
    try:
        CREATE_STREAMS_SCHEMA.validate(schema_to_validate)
    except (ValidationError, BaseException) as ex:
        raise StreamException({'type': JSON_SCHEMA_CREATE_VIOLATION, 'message': ex.__str__()})


def parse_json_delete_stream(schema_to_validate: Any) -> None:
    try:
        DELETE_STREAMS_SCHEMA.validate(schema_to_validate)
    except (ValidationError, BaseException) as ex:
        raise StreamException({'type': JSON_SCHEMA_CREATE_VIOLATION, 'message': ex.__str__()})


def parse_json_line(lines: Any) -> Dict[str, List[Any]]:
    try:
        INSERT_DATA_SCHEMA.validate(lines)
    except (ValidationError, BaseException) as ex:
        raise StreamException({'type': JSON_SCHEMA_LINE_SPLIT_VIOLATION, 'message': ex.__str__()})

    grouped_streams = defaultdict(list)

    for entry in lines:
        concatenated_name = get_context_entry_name(entry['schema'], entry['stream'])
        grouped_streams[concatenated_name].extend(entry['values'])

    return grouped_streams


def validate_json_tuples(stream: BaseIOTStream, tuples: Any) -> Any:
    try:
        stream.get_validation_schema().validate(tuples)
    except (ValidationError, BaseException) as ex:
        raise StreamException({'type': JSON_SCHEMA_INSERT_VIOLATION, 'message': ex.__str__()})
