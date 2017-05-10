import antlr4
import asyncio

from typing import Any, Dict, List

from ingest.monetdb.naming import THREAD_POOL
from ingest.streams.context import get_streams_context
from ingest.streams.streamexception import StreamException, INFLUXDB_LINE_INSERT_VIOLATION
from ingest.tsinfluxline.grammar.influxdbLexer import influxdbLexer
from ingest.tsinfluxline.grammar.influxdbParser import influxdbParser
from ingest.tsinfluxline.guardianlistener import GuardianListener
from ingest.tsinfluxline.linereader import read_chunk_lines, CHUNK_SIZE


def _parse_influxdb_line(line: str, base_tuple_counter: int) -> Dict[str, Any]:
    try:
        lexer = influxdbLexer(antlr4.InputStream(line))
        stream = antlr4.CommonTokenStream(lexer)
        parser = influxdbParser(stream)

        tree = parser.lines()
        printer = GuardianListener(base_tuple_counter)
        walker = antlr4.ParseTreeWalker()
        walker.walk(printer, tree)
    except BaseException as ex:
        raise StreamException({'type': INFLUXDB_LINE_INSERT_VIOLATION, 'message': ex.__str__()})
    return printer.get_grouped_streams()


async def add_influxdb_lines(lines: str) -> List[str]:
    stream_context = get_streams_context()
    base_tuple_counter = 0
    errors = []

    for lines in read_chunk_lines(lines, CHUNK_SIZE):
        try:
            grouped_streams = _parse_influxdb_line(lines, base_tuple_counter)
            for key, values in grouped_streams.items():
                try:  # TODO check UTF-8 strings?
                    stream = stream_context.get_existing_metric(key)
                    await asyncio.wrap_future(THREAD_POOL.submit(stream.insert_values, values['values'],
                                                                 base_tuple_counter))
                except StreamException as ex:
                    errors.append(ex.args[0]['message'])
        except StreamException as ex:
            errors.append(ex.args[0]['message'])

        base_tuple_counter += CHUNK_SIZE

    return errors


async def discovery_influxdb_lines(lines: str) -> List[str]:
    stream_context = get_streams_context()
    base_tuple_counter = 0
    errors = []
    found_streams = set()

    for lines in read_chunk_lines(lines, CHUNK_SIZE):
        try:
            grouped_streams = _parse_influxdb_line(lines, base_tuple_counter)
            for key, values in grouped_streams.items():
                try:  # TODO check UTF-8 strings?
                    found_streams.add(key)
                    stream = stream_context.get_or_add_new_stream_with_influxdb(key, values['tags'], values['values'])
                    await asyncio.wrap_future(THREAD_POOL.submit(stream.insert_values, values['values'],
                                                                 base_tuple_counter))
                except StreamException as ex:
                    errors.append(ex.args[0]['message'])
        except StreamException as ex:
            errors.append(ex.args[0]['message'])

        base_tuple_counter += CHUNK_SIZE

    stream_context.force_flush_streams(found_streams)

    return errors
