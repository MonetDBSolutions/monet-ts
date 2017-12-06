import asyncio
from typing import Any, Dict, List
from jsonschema import ValidationError

from ingest.monetdb.naming import THREAD_POOL
from ingest.streams.streammanager import create_json_stream, delete_json_stream, get_single_json_stream, \
    get_all_json_streams
from ingest.streams.guardianexception import GuardianException, JSON_SCHEMA_CREATE_VIOLATION, \
    JSON_SCHEMA_LINE_SPLIT_VIOLATION, JSON_SCHEMA_DELETE_VIOLATION
from ingest.tsjson.jsoninsert import insert_json_values
from ingest.tsjson.jsonschemas import INSERT_DATA_SCHEMA, CREATE_STREAMS_SCHEMA, DELETE_STREAMS_SCHEMA


async def json_get_single_stream(schema_name: str, stream_name: str) -> Dict[str, Any]:
    return await asyncio.wrap_future(THREAD_POOL.submit(get_single_json_stream, schema_name, stream_name))


async def json_get_all_streams() -> List[Dict[str, Any]]:
    return await asyncio.wrap_future(THREAD_POOL.submit(get_all_json_streams))


async def json_create_stream(submitted_json: Dict[Any, Any]) -> None:
    try:
        CREATE_STREAMS_SCHEMA.validate(submitted_json)
    except (ValidationError, BaseException) as ex:
        raise GuardianException(where=JSON_SCHEMA_CREATE_VIOLATION, message=ex.__str__())

    await asyncio.wrap_future(THREAD_POOL.submit(create_json_stream, submitted_json))


async def json_delete_stream(submitted_json: Dict[str, str]) -> None:
    try:
        DELETE_STREAMS_SCHEMA.validate(submitted_json)
    except (ValidationError, BaseException) as ex:
        raise GuardianException(where=JSON_SCHEMA_DELETE_VIOLATION, message=ex.__str__())

    await asyncio.wrap_future(THREAD_POOL.submit(delete_json_stream, submitted_json))


async def add_json_lines(lines: List[Dict[str, Any]]) -> None:
    try:
        INSERT_DATA_SCHEMA.validate(lines)
    except (ValidationError, BaseException) as ex:
        raise GuardianException(where=JSON_SCHEMA_LINE_SPLIT_VIOLATION, message=ex.__str__())

    await asyncio.wrap_future(THREAD_POOL.submit(insert_json_values, lines))
