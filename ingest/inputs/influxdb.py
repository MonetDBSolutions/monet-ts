from ingest.monetdb.naming import TIMESTAMP_COLUMN_NAME
from ingest.streams.streamexception import StreamException


def parse_influxdb_line(line):
    """
    <metric> <space> [<tagkey>=<tagvalue>,[<tagkey>=<tagvalue>]] <space>
    <fieldkey>=<fieldvalue>[,<fieldkey>=<fieldvalue>] [<timestamp>]
    """
    groups = line.split()

    if len(groups) < 2:
        raise StreamException("The InfluxDB line is incomplete, there should be at least two commas!")

    split_line = groups[0].split(',')

    # get the metric
    metric_full = split_line[0]
    schema_and_stream = metric_full.split('.')
    if len(schema_and_stream) == 0:
        raise StreamException("The measurement field is missing!")
    elif len(schema_and_stream) == 1:
        schema = 'sys'
        stream = schema_and_stream[0]
    else:
        schema = schema_and_stream[0]
        stream = schema_and_stream[1]

    # get the tag columns
    tags = dict()
    for t in split_line[1:]:
        key_value = t.split('=')
        key = key_value[0]
        value = key_value[1]
        tags[key] = value

    # get the value columns
    values = dict()
    for v in groups[1].split(','):
        key_value = v.split('=')
        key = key_value[0]
        value = key_value[1]
        values[key] = value

    if len(groups) > 2:
        values[TIMESTAMP_COLUMN_NAME] = int(groups[2])  # WARNING The timestamp must have seconds precision!!

    return schema, stream, tags, values
