import asyncio
import antlr4

from ingest.monetdb.naming import THREAD_POOL
from ingest.streams.guardianexception import GuardianException, INFLUXDB_LINE_INSERT_VIOLATION
from ingest.tsinfluxline.grammar.influxdbLexer import influxdbLexer
from ingest.tsinfluxline.grammar.influxdbParser import influxdbParser
from ingest.tsinfluxline.guardianinfluxdblistener import GuardianInfluxDBListener
from ingest.tsinfluxline.influxdbinsert import insert_influxdb_values_savage
from ingest.tsjson.jsoninsert import insert_json_values


async def add_influxdb_lines(lines: str) -> None:
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

    values = listener.get_parsed_values()
    await asyncio.wrap_future(THREAD_POOL.submit(insert_json_values, values))

    found_errors = listener.get_found_errors()
    if len(found_errors):
        raise GuardianException(where=INFLUXDB_LINE_INSERT_VIOLATION, message=found_errors)


async def discovery_influxdb_lines(lines: str) -> None:
    await asyncio.wrap_future(THREAD_POOL.submit(insert_influxdb_values_savage, lines, True))
