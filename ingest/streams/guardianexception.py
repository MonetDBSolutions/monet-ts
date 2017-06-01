INFLUXDB_LINE_INSERT_VIOLATION = 1
JSON_SCHEMA_CREATE_VIOLATION = 2
JSON_SCHEMA_LINE_SPLIT_VIOLATION = 3
JSON_SCHEMA_INSERT_VIOLATION = 4
JSON_SCHEMA_DELETE_VIOLATION = 5
STREAM_NOT_FOUND = 6
MAPI_CONNECTION_VIOLATION = 7
MQTT_PROTOCOL_VIOLATION = 8


class GuardianException(Exception):

    def __init__(self, where, message):
        assert where in list(range(1, 9))
        assert type(message) in [str, list, tuple, range]
        self.where = where  # just where in the stack it crashed :(
        self.message = message  # the message itself as an array or string
