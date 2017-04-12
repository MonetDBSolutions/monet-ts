import logging
from handlers.base import BaseHandler

logger = logging.getLogger('boilerplate.' + __name__)


class QueryHandler(BaseHandler):
    def get(self):
        q = self.get_query_argument('q')
        print(self.decode_argument(q))
        self.set_status(200)
        self.write(str(q) + '\n')
        self.finish()

