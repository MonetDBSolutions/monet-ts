import re
from collections import defaultdict
from typing import List

from ingest.monetdb.naming import TIMESTAMP_COLUMN_NAME, METRIC_SEPARATOR, TIMETRAILS_SCHEMA
from ingest.streams.streamexception import StreamException, INFLUXDB_LINE_INSERT_VIOLATION

BOOLEAN_REGEX = re.compile('^t|T|true|True|TRUE|f|F|false|False|FALSE$')
INTEGER_REGEX = re.compile('^[+-]?\d+(?=i)$')
FLOATING_POINT_REGEX = re.compile('^[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?$')
STRING_REGEX = re.compile('^(?<!\\\\)(?:\\\\{2})*"(?:(?<!\\\\)(?:\\\\{2})*\\\\"|[^"])+(?<!\\\\)(?:\\\\{2})*"$')
TAG_REGEX = re.compile('^(?!\s*$).+$')
TIMESTAMP_REGEX = re.compile('^\d+$')


class InfluxDBLineException(Exception):
    pass


def parse_influxdb_line(lines: List[str], base_tuple: int):
    """
    <metric> <space> [<tagkey>=<tagvalue>,[<tagkey>=<tagvalue>]] <space>
    <fieldkey>=<fieldvalue>[,<fieldkey>=<fieldvalue>] [<timestamp>]
    """
    grouped_streams = {}

    for entries in enumerate(lines):
        metric_and_remaining = entries[1].split(',', 1)

        if len(metric_and_remaining) < 2:
            error_message = "The line %d is incomplete, there should be at least two commas!" %\
                            (base_tuple + entries[0])
            raise StreamException({'type': INFLUXDB_LINE_INSERT_VIOLATION, 'message': error_message})

        # get the metric
        metric = metric_and_remaining[0]
        if METRIC_SEPARATOR not in metric:
            metric = TIMETRAILS_SCHEMA + METRIC_SEPARATOR + metric  # by default we will set the line to timetrails

        remaining_data = metric_and_remaining[1].split(' ')
        tags = []

        # get the tag columns
        column_values = defaultdict(list)
        for t in remaining_data[0].split(','):
            key_value = t.split('=')
            key = key_value[0]
            if key not in tags:
                tags.extend(key)
            value = key_value[1]
            column_values[key].extend(value)

        # get the value columns
        for v in remaining_data[1].split(','):
            key_value = v.split('=')
            key = key_value[0]
            if key == TIMESTAMP_COLUMN_NAME:
                raise StreamException({'type': INFLUXDB_LINE_INSERT_VIOLATION,
                                       'message': "The column name %s is reserved?" % TIMESTAMP_COLUMN_NAME})
            value = key_value[1]
            column_values[key].extend(value)

        if len(remaining_data) > 2:
            column_values[TIMESTAMP_COLUMN_NAME].extend(int(remaining_data[2]))

        if metric not in grouped_streams:
            grouped_streams[metric] = {'tags': tags, 'values': column_values}
        else:
            grouped_streams[metric]['values'].extend(column_values)

    return grouped_streams
