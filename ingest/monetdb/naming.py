import multiprocessing
from concurrent.futures import ThreadPoolExecutor

# Identifiers for the  stream types
TUPLE_BASED_STREAM = 1
TIME_BASED_STREAM = 2
AUTO_BASED_STREAM = 3

TIMESTAMP_COLUMN_NAME = 'timestamp'  # the timestamp column to be used by MonetDB

# Start the Thread pool with the number of cores like in MonetDB :)
THREAD_POOL = ThreadPoolExecutor(multiprocessing.cpu_count())


def get_context_entry_name(schema_name, stream_name):  #
    return schema_name + '.' + stream_name
