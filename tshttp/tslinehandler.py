import tornado.web, tornado.escape


class TSBaseLineHandler(tornado.web.RequestHandler):
    """Base line handler for line protocols"""

    def data_received(self, chunk):
        pass

    def write(self, chunk):
        super(TSBaseLineHandler, self).write(tornado.escape.json_encode(chunk))

    def write_error(self, status_code, **kwargs):
        if 'message' not in kwargs:
            if status_code == 405:
                kwargs['message'] = 'Invalid HTTP method.'
            else:
                kwargs['message'] = 'Unknown error.'

        self.write(tornado.escape.json_encode(kwargs))
        self.set_status(status_code)
