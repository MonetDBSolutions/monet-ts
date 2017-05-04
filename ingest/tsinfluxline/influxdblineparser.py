from typing import Any, Dict

import antlr4

from ingest.streams.streamexception import StreamException, INFLUXDB_LINE_INSERT_VIOLATION
from ingest.tsinfluxline.grammar.influxdbLexer import influxdbLexer
from ingest.tsinfluxline.grammar.influxdbParser import influxdbParser
from ingest.tsinfluxline.guardianlistener import GuardianListener


def parse_influxdb_line(line: str, base_tuple_counter: int) -> Dict[str, Any]:
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
