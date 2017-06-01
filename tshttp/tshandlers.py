import tornado.web, tornado.escape

from ingest.streams.guardianexception import GuardianException


class TSBaseHandler(tornado.web.RequestHandler):
    """Our root HTTP handler!"""

    def data_received(self, chunk):
        pass

    def write(self, chunk):
        super(TSBaseHandler, self).write(tornado.escape.json_encode(chunk))

    def write_guardian_exception(self, guardian_exception: GuardianException):
        if isinstance(guardian_exception.message, (list, tuple)):
            self.write('{"messages":["' + '","'.join(guardian_exception.message) + '"]}')
        else:
            self.write('{"message":"' + guardian_exception.message + '"}')

        self.set_header("Content-Type", "application/json")
        self.set_status(400)


class TSJSONHandler(TSBaseHandler):
    """The base handler class for JSON requests"""

    def set_default_headers(self):
        self.set_header('Content-Type', 'application/json')

    def read_body(self):
        return tornado.escape.json_decode(self.request.body)


class TSLineHandler(TSBaseHandler):
    """Base line handler for line protocols"""
