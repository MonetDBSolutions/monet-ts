from collections import defaultdict, OrderedDict
from typing import Dict, Any, List
from pymonetdb import connect

from ingest.monetdb.naming import my_monet_escape
from ingest.streams.guardianexception import GuardianException, MAPI_CONNECTION_VIOLATION, STREAM_NOT_FOUND
from ingest.tsjson.jsonschemas import TIME_WITH_TIMEZONE_TYPE_INTERNAL, TIME_WITH_TIMEZONE_TYPE_EXTERNAL, \
    TIMESTAMP_WITH_TIMEZONE_TYPE_INTERNAL, TIMESTAMP_WITH_TIMEZONE_TYPE_EXTERNAL, BOUNDED_TEXT_INPUTS


def normalize_monetdb_type(monetdb_internal_type: str) -> str:
    if monetdb_internal_type == TIME_WITH_TIMEZONE_TYPE_INTERNAL:
        return TIME_WITH_TIMEZONE_TYPE_EXTERNAL
    elif monetdb_internal_type == TIMESTAMP_WITH_TIMEZONE_TYPE_INTERNAL:
        return TIMESTAMP_WITH_TIMEZONE_TYPE_EXTERNAL
    return monetdb_internal_type


def _create_stream_sql(column_name: str, data_type: str, is_nullable: bool, limit: int=None) -> str:
    null_word = 'NOT NULL' if not is_nullable else 'NULL'
    data_type_real = data_type + "(" + str(limit) + ")" if limit is not None else data_type
    return "%s %s %s" % (my_monet_escape(column_name), data_type_real, null_word)


class PyMonetDBConnection(object):
    def __init__(self, hostname: str, port: int, user_name: str, user_password: str, database: str) -> None:
        self.hostname = hostname
        self.port = port
        self.user_name = user_name
        self.user_password = user_password
        self.database = database
        self.open()

    def open(self):
        self._connection = connect(
            hostname=self.hostname,
            port=self.port,
            username=self.user_name,
            password=self.user_password,
            database=self.database, autocommit=False)
        self._cursor = self._connection.cursor()

    def close(self) -> None:
        self._cursor.close()
        self._connection.close()

    def create_stream(self, schema: str, stream: str, columns: List[Dict[Any, Any]]) -> None:
        validated_columns = []
        for entry in columns:
            validated_columns.append(_create_stream_sql(entry['name'], entry['type'], entry['nullable'],
                                                        entry.get('limit', None)))
        try:
            self._cursor.execute("CREATE SCHEMA IF NOT EXISTS %s" % my_monet_escape(schema))
            self._cursor.execute("CREATE STREAM TABLE %s.%s (%s)" % (my_monet_escape(schema), my_monet_escape(stream),
                                                                     ','.join(validated_columns)))
            self._connection.commit()
        except BaseException as ex:
            self._connection.rollback()
            raise GuardianException(where=MAPI_CONNECTION_VIOLATION, message=ex.__str__())

    def delete_stream(self, schema: str, table: str) -> None:
        try:
            self._cursor.execute("DROP TABLE %s.%s" % (my_monet_escape(schema), my_monet_escape(table)))
            self._connection.commit()
        except BaseException as ex:
            self._connection.rollback()
            raise GuardianException(where=MAPI_CONNECTION_VIOLATION, message=ex.__str__())

    def insert_points_via_csv(self, metric_name: str, nrecords: int, records: str) -> None:
        insert_string = "COPY %d RECORDS INTO %s FROM STDIN;\n%%s" % (nrecords, my_monet_escape(metric_name))
        try:
            self._cursor.execute(insert_string % records)
            self._connection.commit()
        except BaseException as ex:
            self._connection.rollback()
            raise GuardianException(where=MAPI_CONNECTION_VIOLATION, message=ex.__str__())

    def insert_points_via_insertinto(self, schema: str, stream: str, records: str) -> None:
        try:
            self._cursor.execute("INSERT INTO %s.%s VALUES %s" % (my_monet_escape(schema), my_monet_escape(stream),
                                                                  records))
            self._connection.commit()
        except BaseException as ex:
            self._connection.rollback()
            raise GuardianException(where=MAPI_CONNECTION_VIOLATION, message=ex.__str__())

    def get_single_database_stream(self, schema: str, stream: str) -> Dict[Any, Any]:
        """The sql injection should be prevented from the upper layer, but I am doing here as well"""
        try:
            sqlt = ''.join(["""SELECT tables."id", schemas."name", tables."name" FROM""",
                            """(SELECT "id", "name", "schema_id" FROM sys.tables WHERE type='4' AND tables."name"='""",
                            my_monet_escape(stream),
                            """') AS tables INNER JOIN (SELECT "id", "name" FROM sys.schemas WHERE schemas."name"='""",
                            my_monet_escape(schema),
                            """') AS schemas ON (tables."schema_id"=schemas."id") ORDER BY tables."id" """])
            self._cursor.execute(sqlt)
            table = self._cursor.fetchall()
        except BaseException as ex:
            self._connection.rollback()
            raise GuardianException(where=MAPI_CONNECTION_VIOLATION, message=ex.__str__())

        if len(table) == 0:
            raise GuardianException(where=STREAM_NOT_FOUND, message='The stream %s.%s was not found in the server!' %
                                                                    (schema, stream))

        try:
            sqlc = ''.join(["""SELECT columns."table_id", columns."name", columns."type", columns."null",""",
                            """ columns."type_digits" FROM (SELECT "id", "table_id", "name", "type", "null", """,
                            """ "number", "type_digits" FROM sys.columns) AS columns INNER JOIN (SELECT "id" FROM """,
                            """ sys.tables WHERE tables."id"='""", my_monet_escape(str(table[0][0])),
                            """') AS tables ON (tables."id"=columns."table_id") ORDER BY columns."table_id", """
                            """ columns."number" """])
            self._cursor.execute(sqlc)
            columns = self._cursor.fetchall()
            self._connection.commit()
        except BaseException as ex:
            self._connection.rollback()
            raise GuardianException(where=MAPI_CONNECTION_VIOLATION, message=ex.__str__())

        result = OrderedDict([('schema', table[0][1]), ('stream', table[0][2]), ('columns', [])])
        for entry in columns:
            entry = list(entry)
            entry[2] = normalize_monetdb_type(entry[2])
            array_to_append = [('name', entry[1]), ('type', entry[2]), ('nullable', entry[3])]
            if entry[2] in BOUNDED_TEXT_INPUTS:
                array_to_append.append(('limit', entry[4]))
            result['columns'].append(OrderedDict(array_to_append))

        return result

    def get_database_streams(self) -> List[Dict[Any, Any]]:
        results = []

        try:
            tables_sql_string = """SELECT tables."id", schemas."name", tables."name" FROM
                (SELECT "id", "name", "schema_id" FROM sys.tables WHERE type=4) AS tables INNER JOIN (SELECT "id",
                "name" FROM sys.schemas) AS schemas ON (tables."schema_id"=schemas."id") ORDER BY tables."id" """\
                .replace('\n', ' ')
            self._cursor.execute(tables_sql_string)
            tables = self._cursor.fetchall()

            if len(tables) > 0:
                columns_sql_string = """SELECT columns."table_id", columns."name", columns."type", columns."null",
                    columns."type_digits" FROM (SELECT "id", "table_id", "name", "type", "null", "number", "type_digits"
                    FROM sys.columns) AS columns INNER JOIN (SELECT "id" FROM sys.tables WHERE type=4) AS tables ON
                    (tables."id"=columns."table_id") ORDER BY columns."table_id", columns."number" """\
                    .replace('\n', ' ')
                self._cursor.execute(columns_sql_string)
                columns = self._cursor.fetchall()
                self._connection.commit()

                grouped_columns = defaultdict(list)  # group the columns to the respective tables
                for entry in columns:
                    entry = list(entry)
                    entry[2] = normalize_monetdb_type(entry[2])
                    array_to_append = [('name', entry[1]), ('type', entry[2]), ('nullable', entry[3])]
                    if entry[2] in BOUNDED_TEXT_INPUTS:
                        array_to_append.append(('limit', entry[4]))
                    grouped_columns[entry[0]].append(OrderedDict(array_to_append))

                for entry in tables:
                    results.append(OrderedDict([('schema', entry[1]), ('stream', entry[2]),
                                                ('columns', grouped_columns[entry[0]])]))
        except BaseException as ex:
            self._connection.rollback()
            raise GuardianException(where=MAPI_CONNECTION_VIOLATION, message=ex.__str__())

        return results

THIS_MAPI_CONNECTION = None


def init_mapi_connection(con_hostname: str, con_port: int, con_user: str, con_password: str, con_database: str) -> None:
    global THIS_MAPI_CONNECTION
    THIS_MAPI_CONNECTION = PyMonetDBConnection(con_hostname, con_port, con_user, con_password, con_database)


def get_mapi_connection() -> PyMonetDBConnection:
    global THIS_MAPI_CONNECTION
    return THIS_MAPI_CONNECTION
