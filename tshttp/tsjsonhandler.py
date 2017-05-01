import tornado.web, tornado.escape


class TSBaseJSONHandler(tornado.web.RequestHandler):
    """The base handler class for JSON requests"""

    def data_received(self, chunk):
        pass

    def set_default_headers(self):
        self.set_header('Content-Type', 'application/json')

    def read_body(self):
        return tornado.escape.json_decode(self.request.body)

    def write(self, chunk):
        super(TSBaseJSONHandler, self).write(tornado.escape.json_encode(chunk))

    def write_error(self, status_code, **kwargs):
        if 'message' not in kwargs:
            if status_code == 405:
                kwargs['message'] = 'Invalid HTTP method.'
            else:
                kwargs['message'] = 'Unknown error.'

        self.write(kwargs)
        self.set_status(status_code)