import datetime
import multiprocessing

from concurrent.futures import ThreadPoolExecutor
from typing import List

# Start the Thread pool with the number of cores like in MonetDB :)
THREAD_POOL = ThreadPoolExecutor(multiprocessing.cpu_count())

DATABASE_NAME = 'timeseries'
DATABASE_SCHEMA = 'timetrails'
METRIC_SEPARATOR = '.'
TIMESTAMP_COLUMN_NAME = 'ticks'  # the timestamp column to be used by MonetDB

INFLUXDB_TEXT_TYPE = 1
INFLUXDB_BOOL_TYPE = 2
INFLUXDB_INTEGER_TYPE = 3
INFLUXDB_FLOAT_TYPE = 4


def get_default_timestamp_value():
    return datetime.datetime.now().isoformat()


def my_monet_escape(data):
    data = str(data).replace("\\", "\\\\")
    data = data.replace("\'", "\\\'")
    return "%s" % str(data)


def get_metric_name(schema_name: str, stream_name: str) -> str:
    return schema_name + METRIC_SEPARATOR + stream_name


def get_schema_and_stream_name(concat_name: str) -> List[str]:
    return concat_name.split(METRIC_SEPARATOR)[:2]
