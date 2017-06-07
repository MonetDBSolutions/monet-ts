from typing import Any, Dict, List

from ingest.monetdb.mapiconnection import get_mapi_connection, init_mapi_connection
from ingest.monetdb.naming import get_metric_name, get_schema_and_stream_name


class GuardianStreamCache(object):

    def __init__(self) -> None:
        self._cache = {}  # dictionary of metric_name -> column details

    def create_stream(self, schema_name: str, stream_name: str, columns: Any) -> None:
        if self.try_get_stream(schema_name, stream_name) is None:
            get_mapi_connection().create_stream(schema_name, stream_name, columns)
            metric_name = get_metric_name(schema_name, stream_name)
            self._cache[metric_name] = columns

    def delete_stream(self, schema_name: str, stream_name: str) -> None:
        metric_name = get_metric_name(schema_name, stream_name)
        self._cache.pop(metric_name, None)
        get_mapi_connection().delete_stream(schema_name, stream_name)

    def try_get_stream(self, schema_name: str, stream_name: str) -> List[Dict[str, Any]]:
        metric_name = get_metric_name(schema_name, stream_name)
        return self._cache.get(metric_name, None)

    def insert_into_stream(self, schema_name: str, stream_name: str, records: str) -> None:
        get_mapi_connection().insert_points_via_insertinto(schema_name, stream_name, records)

    def get_stream_details_by_metric(self, metric_name: str) -> Dict[str, Any]:
        (schema_name, stream_name) = get_schema_and_stream_name(metric_name)
        return self.get_stream_details_by_schema(schema_name, stream_name)

    def get_stream_details_by_schema(self, schema_name: str, stream_name: str) -> Dict[str, Any]:
        metric_name = get_metric_name(schema_name, stream_name)

        cached_metric = self._cache.get(metric_name, None)
        if cached_metric is None:
            cached_metric = get_mapi_connection().get_single_database_stream(schema_name, stream_name)
            self._cache[metric_name] = cached_metric['columns']

        return cached_metric

    def get_all_streams_details(self) -> List[Dict[str, Any]]:
        database_streams = get_mapi_connection().get_database_streams()

        for entry in database_streams:
            next_metric_name = get_metric_name(entry['schema'], entry['stream'])
            cached_metric = self._cache.get(next_metric_name, None)
            if cached_metric is None:
                self._cache[next_metric_name] = entry['columns']

        return database_streams


STREAM_CACHE = None


def init_streams_context(con_hostname: str, con_port: int, con_user: str, con_password: str, con_database: str) -> None:
    global STREAM_CACHE
    init_mapi_connection(con_hostname, con_port, con_user, con_password, con_database)
    STREAM_CACHE = GuardianStreamCache()
    STREAM_CACHE.get_all_streams_details()  # sync with the database and ignore the returned result


def get_stream_cache() -> GuardianStreamCache:
    global STREAM_CACHE
    return STREAM_CACHE
