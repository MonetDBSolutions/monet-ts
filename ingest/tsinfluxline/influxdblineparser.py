import asyncio

from typing import List

from ingest.monetdb.naming import THREAD_POOL
from ingest.streams.streamexception import StreamException
from ingest.tsinfluxline.influxdbinsert import insert_influxdb_values


async def add_influxdb_lines(lines: str) -> List[str]:
    errors = []

    try:
        await asyncio.wrap_future(THREAD_POOL.submit(insert_influxdb_values, lines, False))
    except StreamException as ex:
        errors.append(ex.args[0]['message'])

    return errors


async def discovery_influxdb_lines(lines: str) -> List[str]:
    errors = []

    try:
        await asyncio.wrap_future(THREAD_POOL.submit(insert_influxdb_values, lines, True))
    except StreamException as ex:
        errors.append(ex.args[0]['message'])

    return errors
