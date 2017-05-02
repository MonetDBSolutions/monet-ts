from collections import defaultdict
from typing import List

from ingest.monetdb.naming import TIMESTAMP_COLUMN_NAME, METRIC_SEPARATOR, TIMETRAILS_SCHEMA
from ingest.streams.streamexception import StreamException, INFLUXDB_LINE_INSERT_VIOLATION


def parse_influxdb_line(lines: List[str], base_tuple: int):
    """
    <metric> <space> [<tagkey>=<tagvalue>,[<tagkey>=<tagvalue>]] <space>
    <fieldkey>=<fieldvalue>[,<fieldkey>=<fieldvalue>] [<timestamp>]
    """
    grouped_streams = defaultdict(list)

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

        # get the tag columns
        column_values = dict()
        for t in remaining_data[0].split(','):
            key_value = t.split('=')
            key = key_value[0]
            value = key_value[1]
            column_values[key] = value

        # get the value columns
        for v in remaining_data[1].split(','):
            key_value = v.split('=')
            key = key_value[0]
            value = key_value[1]
            column_values[key] = value

        if len(remaining_data) > 2:
            column_values[TIMESTAMP_COLUMN_NAME] = int(remaining_data[2])

        grouped_streams[metric].append(column_values)

    return grouped_streams
