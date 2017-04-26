from pymonetdb import connect


class PyMonetDBConnection(object):
    def __init__(self, hostname, port, user_name, user_password, database):
        self._connection = connect(hostname=hostname, port=port, username=user_name, password=user_password,
                                   database=database, autocommit=False)
        self._cursor = self._connection.cursor()

    def close(self):
        self._cursor.close()
        self._connection.close()

    # CREATE TABLE iot.webserverstreams (table_id INTEGER, base INTEGER, "interval" INTEGER NULL, unit INTEGER NULL);
    def get_database_streams(self):
        try:  # TODO put back the WHERE type=4 and the left join
            tables_sql_string = """SELECT tables."id", schemas."name" AS schema, tables."name" AS table, extras."base",
                extras."interval", extras."unit" FROM (SELECT "id", "name", "schema_id" FROM sys.tables)
                AS tables INNER JOIN (SELECT "id", "name" FROM sys.schemas) AS schemas ON
                (tables."schema_id"=schemas."id") INNER JOIN (SELECT "table_id", "base", "interval", "unit" FROM
                iot.webserverstreams) AS extras ON (tables."id"=extras."table_id") ORDER BY tables."id" """\
                .replace('\n', ' ')
            self._cursor.execute(tables_sql_string)
            tables = self._cursor.fetchall()

            # TODO WHERE type=4
            columns_sql_string = """SELECT columns."id", columns."table_id", columns."name" AS column, columns."type",
                columns."type_digits", columns."type_scale", columns."null" FROM (SELECT "id",
                "table_id", "name", "type", "type_digits", "type_scale", "null", "number" FROM sys.columns)
                AS columns INNER JOIN (SELECT "id" FROM sys.tables) AS tables ON
                (tables."id"=columns."table_id") ORDER BY columns."table_id", columns."number" """.replace('\n', ' ')
            self._cursor.execute(columns_sql_string)
            columns = self._cursor.fetchall()

            self._connection.commit()
            return tables, columns
        except BaseException:
            self._connection.rollback()
            raise

    def create_stream(self, schema, stream, columns, flush_statement):
        try:
            try:  # TODO CREATE SCHEMA IF NOT EXISTS
                self._cursor.execute("CREATE SCHEMA %s" % schema)
                self._connection.commit()
            except:
                self._connection.rollback()

            # TODO put STREAM back!!
            self._cursor.execute("CREATE TABLE %s.%s (%s)" % (schema, stream, columns))
            self._cursor.execute("SELECT id FROM sys.schemas WHERE \"name\"='%s'" % schema)
            schema_id = self._cursor.fetchall()[0][0]
            self._cursor.execute("SELECT id FROM sys.tables WHERE schema_id=%s AND \"name\"='%s'" % (schema_id, stream))
            table_id = self._cursor.fetchall()[0][0]
            self._cursor.execute("INSERT INTO iot.webserverstreams VALUES (%s,%s)" % (table_id, flush_statement))

            self._connection.commit()
            return table_id
        except BaseException:
            self._connection.rollback()
            raise

    def delete_stream(self, schema, table, stream_id):
        try:
            self._cursor.execute("DROP TABLE %s.%s" % (schema, table))
            self._cursor.execute("DELETE FROM iot.webserverstreams WHERE table_id='%s'" % stream_id)
            self._connection.commit()
        except BaseException:
            self._connection.rollback()
            raise

    def insert_points(self, schema, stream, points):
        insert_string = "INSERT INTO %s.%s VALUES %%s" % (schema, stream)
        try:
            self._cursor.execute(insert_string % points)
            self._connection.commit()
        except BaseException:
            self._connection.rollback()
            raise
