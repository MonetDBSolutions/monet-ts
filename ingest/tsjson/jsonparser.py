import asyncio
from typing import Any, Dict, List
from jsonschema import ValidationError

from ingest.monetdb.naming import THREAD_POOL
from ingest.streams.streamcreator import create_json_stream, delete_json_stream
from ingest.streams.streamexception import StreamException, JSON_SCHEMA_CREATE_VIOLATION, \
    JSON_SCHEMA_LINE_SPLIT_VIOLATION, JSON_SCHEMA_DELETE_VIOLATION
from ingest.tsjson.jsoninsert import insert_json_values
from ingest.tsjson.jsonschemas import INSERT_DATA_SCHEMA, CREATE_STREAMS_SCHEMA, DELETE_STREAMS_SCHEMA


async def json_create_stream(submitted_json: Dict[Any, Any]) -> None:
    try:
        try:
            CREATE_STREAMS_SCHEMA.validate(submitted_json)
        except (ValidationError, BaseException) as ex:
            raise StreamException({'type': JSON_SCHEMA_CREATE_VIOLATION, 'message': ex.__str__()})
        await asyncio.wrap_future(THREAD_POOL.submit(create_json_stream, submitted_json))
    except StreamException as ex:
        return ex.args[0]['message']


async def json_delete_stream(submitted_json: Dict[str, str]) -> None:
    try:
        try:
            DELETE_STREAMS_SCHEMA.validate(submitted_json)
        except (ValidationError, BaseException) as ex:
            raise StreamException({'type': JSON_SCHEMA_DELETE_VIOLATION, 'message': ex.__str__()})
        await asyncio.wrap_future(THREAD_POOL.submit(delete_json_stream, submitted_json))
    except StreamException as ex:
        return ex.args[0]['message']


async def add_json_lines(lines: List[Any]) -> List[str]:
    errors = []

    try:
        INSERT_DATA_SCHEMA.validate(lines)
    except (ValidationError, BaseException) as ex:
        raise StreamException({'type': JSON_SCHEMA_LINE_SPLIT_VIOLATION, 'message': ex.__str__()})

    try:
        for values in lines:
            try:
                await asyncio.wrap_future(THREAD_POOL.submit(insert_json_values, values))
            except StreamException as ex:
                errors.append(ex.args[0]['message'])
    except StreamException as ex:
        errors.append(ex.args[0]['message'])

    return errors
