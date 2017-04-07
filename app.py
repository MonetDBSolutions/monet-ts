#!/usr/bin/env python

import tornado.httpserver
import tornado.ioloop
import tornado.web
import logging
from tornado.options import options

from settings import settings
from urls import url_patterns

logger = logging.getLogger('boilerplate.' + __name__)

class TornadoBoilerplate(tornado.web.Application):
    def __init__(self):
        tornado.web.Application.__init__(self, url_patterns, **settings)


def main():
    app = TornadoBoilerplate()
    http_server = tornado.httpserver.HTTPServer(app)
    http_server.listen(options.port)
    logger.info("Monet Time Stream server listening on port " + str(options.port) + "...")
    tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    main()
