import asyncio
import antlr4

from ingest.monetdb.naming import THREAD_POOL
from ingest.streams.guardianexception import GuardianException, INFLUXDB_LINE_INSERT_VIOLATION
from ingest.streams.streammanager import create_stream_from_influxdb_discovery_slow
from ingest.tsinfluxline.grammar.influxdbLexer import influxdbLexer
from ingest.tsinfluxline.grammar.influxdbParser import influxdbParser
from ingest.tsinfluxline.guardianinfluxdblistener import GuardianInfluxDBListener
from ingest.tsinfluxline.influxdbinsert import insert_influxdb_values_savage
from ingest.tsjson.jsoninsert import insert_json_values


def use_antlr_parser(lines: str):
    try:
        lexer = influxdbLexer(antlr4.InputStream(lines))
        stream = antlr4.CommonTokenStream(lexer)
        parser = influxdbParser(stream)

        tree = parser.lines()
        listener = GuardianInfluxDBListener()
        walker = antlr4.ParseTreeWalker()
        walker.walk(listener, tree)
    except BaseException as ex:
        raise GuardianException(where=INFLUXDB_LINE_INSERT_VIOLATION, message=ex.__str__())

    return listener.get_parsed_values(), listener.get_found_errors()


async def add_influxdb_lines(lines: str) -> None:
    (streams, found_errors) = use_antlr_parser(lines)
    await asyncio.wrap_future(THREAD_POOL.submit(insert_json_values, streams))

    if len(found_errors):
        raise GuardianException(where=INFLUXDB_LINE_INSERT_VIOLATION, message=found_errors)


async def discovery_influxdb_lines_fast(lines: str) -> None:
    await asyncio.wrap_future(THREAD_POOL.submit(insert_influxdb_values_savage, lines, True))


async def discovery_influxdb_lines_slow(lines: str) -> None:
    (streams, found_errors) = use_antlr_parser(lines)

    for values in streams:
        create_stream_from_influxdb_discovery_slow(values['schema'], values['stream'], values['values'][0],
                                                   values['tags'])

    await asyncio.wrap_future(THREAD_POOL.submit(insert_json_values, streams))

    if len(found_errors):
        raise GuardianException(where=INFLUXDB_LINE_INSERT_VIOLATION, message=found_errors)
