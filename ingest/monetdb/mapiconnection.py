from typing import Tuple, Any
from pymonetdb import connect

from ingest.streams.streamexception import StreamException, MAPI_CONNECTION


class PyMonetDBConnection(object):
    def __init__(self, hostname: str, port: int, user_name: str, user_password: str, database: str) -> None:
        self._connection = connect(hostname=hostname, port=port, username=user_name, password=user_password,
                                   database=database, autocommit=False)
        self._cursor = self._connection.cursor()

    def init_timetrails(self) -> None:
        try:
            stream_table = """CREATE TABLE IF NOT EXISTS timetrails.webserverstreams (table_id INTEGER, base INTEGER,
            "interval" INTEGER, unit INTEGER)""".replace('\n', ' ')
            self._cursor.execute(stream_table)
            self._connection.commit()
        except:
            self._connection.rollback()

    def close(self) -> None:
        self._cursor.close()
        self._connection.close()

    def get_database_streams(self) -> Tuple[Any, Any]:
        try:
            tables_sql_string = """SELECT tables."id", schemas."name" AS schema, tables."name" AS table, extras."base",
                extras."interval", extras."unit" FROM (SELECT "id", "name", "schema_id" FROM sys.tables WHERE type=4)
                AS tables INNER JOIN (SELECT "id", "name" FROM sys.schemas) AS schemas ON
                (tables."schema_id"=schemas."id") INNER JOIN (SELECT "table_id", "base", "interval", "unit" FROM
                timetrails.webserverstreams) AS extras ON (tables."id"=extras."table_id") ORDER BY tables."id" """\
                .replace('\n', ' ')
            self._cursor.execute(tables_sql_string)
            tables = self._cursor.fetchall()

            columns_sql_string = """SELECT columns."id", columns."table_id", columns."name" AS column, columns."type",
                columns."type_digits", columns."type_scale", columns."null" FROM (SELECT "id",
                "table_id", "name", "type", "type_digits", "type_scale", "null", "number" FROM sys.columns)
                AS columns INNER JOIN (SELECT "id" FROM sys.tables WHERE type=4) AS tables ON
                (tables."id"=columns."table_id") ORDER BY columns."table_id", columns."number" """.replace('\n', ' ')
            self._cursor.execute(columns_sql_string)
            columns = self._cursor.fetchall()

            self._connection.commit()
            return tables, columns
        except BaseException as ex:
            self._connection.rollback()
            raise StreamException({'type': MAPI_CONNECTION, 'message': ex.__str__()})

    def create_stream(self, schema: str, stream: str, columns: str, flush_statement: str) -> str:
        try:
            self._cursor.execute("CREATE SCHEMA IF NOT EXISTS \"%s\"" % schema)
            self._cursor.execute("CREATE STREAM TABLE \"%s\".\"%s\" (%s)" % (schema, stream, columns))
            self._cursor.execute("SELECT id FROM sys.schemas WHERE \"name\"='%s'" % schema)
            schema_id = self._cursor.fetchall()[0][0]
            self._cursor.execute("SELECT id FROM sys.tables WHERE schema_id=%s AND \"name\"='%s'" % (schema_id, stream))
            table_id = self._cursor.fetchall()[0][0]
            self._cursor.execute("INSERT INTO timetrails.webserverstreams VALUES (%s,%s)" % (table_id, flush_statement))

            self._connection.commit()
            return table_id
        except BaseException as ex:
            self._connection.rollback()
            raise StreamException({'type': MAPI_CONNECTION, 'message': ex.__str__()})

    def delete_stream(self, schema: str, table: str, stream_id: str) -> None:
        try:
            self._cursor.execute("DROP TABLE \"%s\".\"%s\"" % (schema, table))
            self._cursor.execute("DELETE FROM timetrails.webserverstreams WHERE table_id='%s'" % stream_id)
            self._connection.commit()
        except BaseException as ex:
            self._connection.rollback()
            raise StreamException({'type': MAPI_CONNECTION, 'message': ex.__str__()})

    def insert_points(self, schema: str, stream: str, points: str) -> None:
        insert_string = "INSERT INTO \"%s\".\"%s\" VALUES %%s" % (schema, stream)
        try:
            self._cursor.execute(insert_string % points)
            self._connection.commit()
        except BaseException as ex:
            self._connection.rollback()
            raise StreamException({'type': MAPI_CONNECTION, 'message': ex.__str__()})
