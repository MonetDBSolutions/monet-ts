import datetime

from ingest.monetdb.mapiconnection import get_mapi_connection
from ingest.monetdb.naming import DATABASE_SCHEMA, get_default_timestamp_value, INFLUXDB_TEXT_TYPE, \
    INFLUXDB_BOOL_TYPE, INFLUXDB_INTEGER_TYPE, INFLUXDB_FLOAT_TYPE
from ingest.streams.streammanager import create_stream_from_influxdb

CHUNK_SIZE = 100000


def flush(discovery, found_metrics, values_to_insert):
    if discovery:
        for metric, values in found_metrics.items():
            if not values['created']:
                create_stream_from_influxdb(metric, values['columns'])
                values['created'] = True

    for metric, values in values_to_insert.items():
        # "COPY 2 RECORDS INTO test FROM STDIN;\n44444444|AL|1495544891\n55555555|JAFFRI"
        get_mapi_connection().insert_points_via_csv(metric, len(values), '\n'.join(values))


# WARNING this has a lot of security holes!!
# sys.testing,location=us a=12i,b="hello" 1493109338000
def insert_influxdb_values_savage(lines: str, discovery: bool=False) -> None:
    time_to_input = get_default_timestamp_value()  # if the timestamp is missing
    values_to_insert = {}
    found_metrics = {}
    i = 0
    line_number = 0
    length_lines = len(lines)
    inserts = []

    while i < length_lines:
        # parse the metrics
        j = i
        while lines[i] not in ('.', ',', ' '):
            i += 1
        if lines[i] == '.':
            while lines[i] not in (',', ' '):
                i += 1
            metric_name = lines[j:i]
        else:
            metric_name = DATABASE_SCHEMA + '.' + lines[j:i]

        if metric_name not in found_metrics:
            values_to_insert[metric_name] = []
            if discovery:
                found_metrics[metric_name] = {'columns': {}, 'created': False}
            new_metric = True
        else:
            new_metric = False

        del inserts[:]

        # parse the tags
        if lines[i] == ',':
            i += 1
            while lines[i] != ' ':
                j = i
                while lines[i] != '=':
                    i += 1
                if new_metric:
                    next_tag_name = lines[(j+1):i]
                    found_metrics[metric_name]['columns'][next_tag_name] = {'type': INFLUXDB_TEXT_TYPE, 'isTag': True}

                i += 1
                j = i
                while lines[i] not in (',', ' '):
                    i += 1
                inserts.append(lines[j:i])

        i += 1
        # parse the columns
        while lines[i] not in (' ', '\n'):
            j = i
            while lines[i] != '=':
                i += 1
            if new_metric and discovery:
                next_column_name = lines[(j+1):i]

            i += 1
            j = i
            while lines[i] not in (',', ' '):
                i += 1
            next_insert = lines[j:i]

            if next_insert in ('t', 'T', 'true', 'True', 'TRUE'):
                inserts.append('1')
                if new_metric and discovery:
                    found_metrics[metric_name]['columns'][next_column_name] = {'type': INFLUXDB_BOOL_TYPE, 'isTag': False}
            if next_insert in ('f', 'F', 'false', 'False', 'FALSE'):
                inserts.append('0')
                if new_metric and discovery:
                    found_metrics[metric_name]['columns'][next_column_name] = {'type': INFLUXDB_BOOL_TYPE, 'isTag': False}
            elif next_insert.startswith('"'):
                inserts.append(next_insert[1:-1].replace('\\"', '"'))
                if new_metric and discovery:
                    found_metrics[metric_name]['columns'][next_column_name] = {'type': INFLUXDB_TEXT_TYPE, 'isTag': False}
            elif next_insert.endswith('i'):
                inserts.append(next_insert[:-1])
                if new_metric and discovery:
                    found_metrics[metric_name]['columns'][next_column_name] = {'type': INFLUXDB_INTEGER_TYPE, 'isTag': False}
            else:
                inserts.append(next_insert)
                if new_metric and discovery:
                    found_metrics[metric_name]['columns'][next_column_name] = {'type': INFLUXDB_FLOAT_TYPE, 'isTag': False}

        # parse the timestamp
        if lines[i] == ' ':
            i += 1
            j = i
            while i < length_lines and lines[i].isdigit():
                i += 1
            unix_timestamp = int(lines[j:i-9])
            parsed_timestamp = datetime.datetime.fromtimestamp(unix_timestamp).isoformat()
        else:
            parsed_timestamp = time_to_input

        inserts.append(parsed_timestamp)
        values_to_insert[metric_name].append('|'.join(inserts))

        if i < length_lines and lines[i] == '\n':
            i += 1

        line_number += 1
        if line_number == CHUNK_SIZE:
            flush(discovery, found_metrics, values_to_insert)
            values_to_insert = {}
            line_number = 0

    flush(discovery, found_metrics, values_to_insert)  # at the end, flush the remaining lines
