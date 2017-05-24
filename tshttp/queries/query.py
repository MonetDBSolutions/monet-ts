import logging
import pymonetdb

from tshttp.tsjsonhandler import TSBaseJSONHandler

logger = logging.getLogger('boilerplate.' + __name__)


class QueryHandler(TSBaseJSONHandler):
    def initialize(self, dbConnection):
        self.dbConnection = dbConnection
    def prepare(self):
        print("[QueryHandler] -- received request")
        self.dbConnection.open()
    def get(self):
        self.setCORSHeaders()
        try:
            results = self.db_fetch()
            self.set_status(200)
            self.write({
                'results': results
            })
        except pymonetdb.exceptions.OperationalError as err:
            self.set_status(200)
            self.write({
                'results': []
            })
            print(err)
        except pymonetdb.exceptions.DatabaseError as err:
            self.set_status(503)
            self.write({
                'error': 'DatabaseError',
                'message': 'Error while processing query results!'
            })
        self.finish()

    def db_fetch(self):
        results = []
        cursor = self.dbConnection._cursor
        queries = self.get_query_argument('q').split(';')
        for q in queries:
            print(q)
            queryResult = cursor.execute(q)
            values = cursor.fetchall()
            print(values)
            results.append({
                'series': {
                    'values': values
                }
            })
        return results

    def on_finish(self):
        self.dbConnection.close()

    def setCORSHeaders(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Methods", "POST")
        self.set_header("Access-Control-Allow-Methods", "GET")
        self.set_header("Access-Control-Allow-Headers", "accept, content-type")

