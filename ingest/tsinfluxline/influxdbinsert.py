import datetime
from collections import OrderedDict

from ingest.monetdb.mapiconnection import get_mapi_connection
from ingest.monetdb.naming import DATABASE_SCHEMA, get_default_timestamp_value, INFLUXDB_TEXT_TYPE, \
    INFLUXDB_BOOL_TYPE, INFLUXDB_INTEGER_TYPE, INFLUXDB_FLOAT_TYPE
from ingest.streams.streammanager import create_stream_from_influxdb_discovery_fast

CHUNK_SIZE = 100000


def flush(discovery, found_metrics, values_to_insert):
    if discovery:
        for metric, values in found_metrics.items():
            if not values['created']:
                tuples_array = map(lambda x: (x['name'], {'type': x['type'], 'isTag': x['isTag']}), values['columns'])
                values['columns'] = OrderedDict(tuples_array)
                create_stream_from_influxdb_discovery_fast(metric, values['columns'])
                values['created'] = True

    for metric, values in values_to_insert.items():
        # "COPY 2 RECORDS INTO test FROM STDIN;\n44444444|AL|1495544891\n55555555|JAFFRI"
        get_mapi_connection().insert_points_via_csv(metric, len(values), '\n'.join(values))


# WARNING this has a lot of holes!!
# sys.testing,location=us a=12i,b="hello" 1493109338000
def insert_influxdb_values_savage(lines: str, discovery: bool=False) -> None:
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
                found_metrics[metric_name] = {'columns': [], 'created': False}
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
                    next_tag_name = lines[j:i]
                    found_metrics[metric_name]['columns'].append({'name': next_tag_name,
                                                                  'type': INFLUXDB_TEXT_TYPE, 'isTag': True})

                i += 1
                j = i
                while lines[i] not in (',', ' '):
                    i += 1
                inserts.append(lines[j:i])

                if lines[i] == ',':
                    i += 1

        i += 1
        # parse the columns
        while i < length_lines and lines[i] not in (' ', '\n'):
            j = i
            while lines[i] != '=':
                i += 1
            if new_metric and discovery:
                next_column_name = lines[j:i]

            i += 1
            j = i
            while i < length_lines and lines[i] not in (',', ' '):
                i += 1
            next_insert = lines[j:i]

            if next_insert in ('t', 'T', 'true', 'True', 'TRUE'):
                inserts.append('1')
                if new_metric and discovery:
                    found_metrics[metric_name]['columns'].append({'name': next_column_name,
                                                                  'type': INFLUXDB_BOOL_TYPE, 'isTag': False})
            if next_insert in ('f', 'F', 'false', 'False', 'FALSE'):
                inserts.append('0')
                if new_metric and discovery:
                    found_metrics[metric_name]['columns'].append({'name': next_column_name,
                                                                  'type': INFLUXDB_BOOL_TYPE, 'isTag': False})
            elif next_insert.startswith('"'):
                inserts.append(next_insert[1:-1].replace('\\"', '"'))
                if new_metric and discovery:
                    found_metrics[metric_name]['columns'].append({'name': next_column_name,
                                                                  'type': INFLUXDB_TEXT_TYPE, 'isTag': False})
            elif next_insert.endswith('i'):
                inserts.append(next_insert[:-1])
                if new_metric and discovery:
                    found_metrics[metric_name]['columns'].append({'name': next_column_name,
                                                                  'type': INFLUXDB_INTEGER_TYPE, 'isTag': False})
            else:
                inserts.append(next_insert)
                if new_metric and discovery:
                    found_metrics[metric_name]['columns'].append({'name': next_column_name,
                                                                  'type': INFLUXDB_FLOAT_TYPE, 'isTag': False})

            i += 1

        # parse the timestamp
        if i < length_lines and lines[i] == ' ':
            i += 1
            j = i
            while i < length_lines and lines[i].isdigit():
                i += 1
            unix_timestamp = int(lines[j:i-9])
            parsed_timestamp = datetime.datetime.fromtimestamp(unix_timestamp).isoformat()
        else:
            parsed_timestamp = get_default_timestamp_value()

        inserts.append(parsed_timestamp)
        values_to_insert[metric_name].append('|'.join(inserts))

        while i < length_lines and lines[i] == '\n':
            i += 1

        line_number += 1
        if line_number == CHUNK_SIZE:
            flush(discovery, found_metrics, values_to_insert)
            values_to_insert = {}
            line_number = 0

    flush(discovery, found_metrics, values_to_insert)  # at the end, flush the remaining lines
