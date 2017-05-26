import datetime
import multiprocessing
from concurrent.futures import ThreadPoolExecutor

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
INFLUXDB_TIMESTAMP_TYPE = 5


def get_default_timestamp_value():
    return datetime.datetime.now().isoformat()
