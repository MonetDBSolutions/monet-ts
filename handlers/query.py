import logging
import pymonetdb

from handlers.base import BaseHandler

logger = logging.getLogger('boilerplate.' + __name__)
connection = pymonetdb.connect(
    username = "monetdb",
    password = "monetdb",
    hostname = "localhost",
    database = "timeseries")

cursor = connection.cursor()

class QueryHandler(BaseHandler):
    def get(self):
        q = self.get_query_argument('q')
        print(self.decode_argument(q))
        result_count = cursor.execute(q)
        results = cursor.fetchall()
        print(results)
        self.set_status(200)
        self.finish()

