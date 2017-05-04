import multiprocessing
from concurrent.futures import ThreadPoolExecutor

# Start the Thread pool with the number of cores like in MonetDB :)
from typing import List

THREAD_POOL = ThreadPoolExecutor(multiprocessing.cpu_count())

# Identifiers for the stream types
TUPLE_BASED_STREAM = 1
TIME_BASED_STREAM = 2
AUTO_BASED_STREAM = 3

METRIC_SEPARATOR = '.'
TIMETRAILS_SCHEMA = 'timetrails'
TIMESTAMP_COLUMN_NAME = 'ticks'  # the timestamp column to be used by MonetDB


def get_context_entry_name(schema_name: str, stream_name: str) -> str:
    return schema_name + METRIC_SEPARATOR + stream_name


def get_schema_and_stream_name(concat_name: str) -> List[str]:
    return concat_name.split(METRIC_SEPARATOR)[:2]
