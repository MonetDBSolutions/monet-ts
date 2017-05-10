import asyncio
from collections import defaultdict
from typing import Any, Dict, List
from jsonschema import ValidationError

from ingest.monetdb.naming import get_context_entry_name, THREAD_POOL
from ingest.streams.context import get_streams_context
from ingest.streams.streamexception import StreamException, JSON_SCHEMA_CREATE_VIOLATION, \
    JSON_SCHEMA_LINE_SPLIT_VIOLATION, JSON_SCHEMA_INSERT_VIOLATION
from ingest.tsjson.jsonschemas import INSERT_DATA_SCHEMA, CREATE_STREAMS_SCHEMA, DELETE_STREAMS_SCHEMA


async def json_create_stream(submitted_json: Any) -> None:
    try:
        try:
            CREATE_STREAMS_SCHEMA.validate(submitted_json)
        except (ValidationError, BaseException) as ex:
            raise StreamException({'type': JSON_SCHEMA_CREATE_VIOLATION, 'message': ex.__str__()})
        stream_context = get_streams_context()
        await asyncio.wrap_future(THREAD_POOL.submit(stream_context.add_new_stream_with_json, submitted_json))
    except StreamException as ex:
        return ex.args[0]['message']


async def json_delete_stream(submitted_json: Any) -> None:
    try:
        try:
            DELETE_STREAMS_SCHEMA.validate(submitted_json)
        except (ValidationError, BaseException) as ex:
            raise StreamException({'type': JSON_SCHEMA_CREATE_VIOLATION, 'message': ex.__str__()})
        stream_context = get_streams_context()
        await asyncio.wrap_future(THREAD_POOL.submit(stream_context.delete_existing_stream, submitted_json))
    except StreamException as ex:
        return ex.args[0]['message']


def _parse_json_line(lines: Any) -> Dict[str, List[Any]]:
    try:
        INSERT_DATA_SCHEMA.validate(lines)
    except (ValidationError, BaseException) as ex:
        raise StreamException({'type': JSON_SCHEMA_LINE_SPLIT_VIOLATION, 'message': ex.__str__()})

    grouped_streams = defaultdict(list)

    for entry in lines:
        concatenated_name = get_context_entry_name(entry['schema'], entry['stream'])
        grouped_streams[concatenated_name].extend(entry['values'])

    return grouped_streams


async def add_json_lines(lines: str) -> List[str]:
    stream_context = get_streams_context()
    errors = []
    try:
        grouped_streams = _parse_json_line(lines)
        for key, values in grouped_streams.items():
            try:
                metric = stream_context.get_existing_metric(key)
                try:
                    metric.get_validation_schema().validate(values)
                except (ValidationError, BaseException) as ex:
                    raise StreamException({'type': JSON_SCHEMA_INSERT_VIOLATION, 'message': ex.__str__()})
                await asyncio.wrap_future(THREAD_POOL.submit(metric.insert_values, values, 0))
            except StreamException as ex:
                errors.append(ex.args[0]['message'])
    except StreamException as ex:
        errors.append(ex.args[0]['message'])

    return errors
