from collections import defaultdict, OrderedDict
from typing import Dict, Any

from pymonetdb import connect
from ingest.streams.streamexception import StreamException, MAPI_CONNECTION


class PyMonetDBConnection(object):
    def __init__(self, hostname: str, port: int, user_name: str, user_password: str, database: str) -> None:
        self._connection = connect(hostname=hostname, port=port, username=user_name, password=user_password,
                                   database=database, autocommit=False)
        self._cursor = self._connection.cursor()

    def close(self) -> None:
        self._cursor.close()
        self._connection.close()

    def create_stream(self, schema: str, stream: str, columns: str) -> None:
        try:
            self._cursor.execute("CREATE SCHEMA IF NOT EXISTS \"%s\"" % schema)
            self._cursor.execute("CREATE STREAM TABLE \"%s\".\"%s\" (%s)" % (schema, stream, columns))
            self._connection.commit()
        except BaseException as ex:
            self._connection.rollback()
            raise StreamException({'type': MAPI_CONNECTION, 'message': ex.__str__()})

    def delete_stream(self, schema: str, table: str) -> None:
        try:
            self._cursor.execute("DROP TABLE \"%s\".\"%s\"" % (schema, table))
            self._connection.commit()
        except BaseException as ex:
            self._connection.rollback()
            raise StreamException({'type': MAPI_CONNECTION, 'message': ex.__str__()})

    def insert_points(self, metric_name: str, nrecords: int, records: str) -> None:
        insert_string = "COPY %d RECORDS INTO %s FROM STDIN;\n%%s" % (nrecords, metric_name)
        try:
            self._cursor.execute(insert_string % records)
            self._connection.commit()
        except BaseException as ex:
            self._connection.rollback()
            raise StreamException({'type': MAPI_CONNECTION, 'message': ex.__str__()})

    def get_single_database_stream(self, schema: str, stream: str) -> Dict[Any, Any]:
        try:
            sqlt = ''.join(["""SELECT tables."id", schemas."name", tables."name" FROM""",
                            """(SELECT "id", "name", "schema_id" FROM sys.tables WHERE type='4' AND tables."name"='""",
                            stream,
                            """') AS tables INNER JOIN (SELECT "id", "name" FROM sys.schemas WHERE schemas."name"='""",
                            schema, """') AS schemas ON (tables."schema_id"=schemas."id") ORDER BY tables."id" """])
            self._cursor.execute(sqlt)
            table = self._cursor.fetchall()

            sqlc = ''.join(["""SELECT columns."table_id", columns."name", columns."type", columns."null" FROM """,
                            """(SELECT "id", "table_id", "name", "type", "null", "number" FROM sys.columns)""",
                            """ AS columns INNER JOIN (SELECT "id" FROM sys.tables WHERE tables."id"='""",
                            str(table[0][0]), """') AS tables ON (tables."id"=columns."table_id")"""
                                              """ ORDER BY columns."table_id", columns."number" """])
            self._cursor.execute(sqlc)
            columns = self._cursor.fetchall()
            self._connection.commit()

            result = OrderedDict([('schema', table[0][1]), ('stream', table[0][2]), ('columns', [])])
            for entry in columns:
                result['columns'].append(OrderedDict([('name', entry[1]), ('type', entry[2]), ('nullable', entry[3])]))

            return result
        except BaseException as ex:
            self._connection.rollback()
            raise StreamException({'type': MAPI_CONNECTION, 'message': ex.__str__()})

    def get_database_streams(self) -> Dict[Any, Any]:
        try:
            tables_sql_string = """SELECT tables."id", schemas."name", tables."name" FROM
                (SELECT "id", "name", "schema_id" FROM sys.tables WHERE type=4) AS tables INNER JOIN (SELECT "id",
                "name" FROM sys.schemas) AS schemas ON (tables."schema_id"=schemas."id") ORDER BY tables."id" """\
                .replace('\n', ' ')
            self._cursor.execute(tables_sql_string)
            tables = self._cursor.fetchall()

            columns_sql_string = """SELECT columns."table_id", columns."name", columns."type",
                columns."null" FROM (SELECT "id", "table_id", "name", "type", "null", "number" FROM sys.columns)
                AS columns INNER JOIN (SELECT "id" FROM sys.tables WHERE type=4) AS tables ON
                (tables."id"=columns."table_id") ORDER BY columns."table_id", columns."number" """.replace('\n', ' ')
            self._cursor.execute(columns_sql_string)
            columns = self._cursor.fetchall()
            self._connection.commit()

            grouped_columns = defaultdict(list)  # group the columns to the respective tables
            for entry in columns:
                grouped_columns[entry[0]].append(OrderedDict([('name', entry[1]), ('type', entry[2]),
                                                              ('nullable', entry[3])]))

            results = []
            for entry in tables:
                results.append(OrderedDict([('schema', entry[1]), ('stream', entry[2]),
                                            ('columns', grouped_columns[entry[0]])]))

            return {'streams_count': len(results), 'streams_listing': results}
        except BaseException as ex:
            self._connection.rollback()
            raise StreamException({'type': MAPI_CONNECTION, 'message': ex.__str__()})


THIS_MAPI_CONNECTION = None


def init_mapi_connection(con_hostname: str, con_port: int, con_user: str, con_password: str, con_database: str) -> None:
    global THIS_MAPI_CONNECTION
    THIS_MAPI_CONNECTION = PyMonetDBConnection(con_hostname, con_port, con_user, con_password, con_database)


def get_mapi_connection() -> PyMonetDBConnection:
    global THIS_MAPI_CONNECTION
    return THIS_MAPI_CONNECTION
