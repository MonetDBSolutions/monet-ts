import logging
from handlers.base import BaseHandler

logger = logging.getLogger('boilerplate.' + __name__)


class QueryHandler(BaseHandler):
    def get(self):
        self.write('Hello from QueryHandler!\n')

