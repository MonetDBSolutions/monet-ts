from abc import ABCMeta
from datetime import datetime, date, timedelta


class StreamDataType(object):
    """MonetDB's data types for validation base class"""
    __metaclass__ = ABCMeta

    def __init__(self, **kwargs):
        self._column_name = kwargs['name']  # name of the column
        self._data_type = kwargs['type']  # SQL name of the type
        self._is_nullable = kwargs.get('nullable', True)  # boolean

    def is_nullable(self):  # check if the column is nullable or not
        return self._is_nullable

    def add_json_schema_entry(self, schema):  # add the entry for the stream's corresponding json schema
        schema[self._column_name] = {}

    def to_json_representation(self):  # get a json representation of the data type while checking the stream's info
        return {'name': self._column_name, 'type': self._data_type, 'nullable': self._is_nullable}

    def create_stream_sql(self):  # get column creation statement on SQL
        null_word = 'NOT NULL' if not self._is_nullable else 'NULL'
        return self._column_name + " " + self._data_type + " " + null_word

    def convert_value_into_sql(self, new_value):
        return new_value if type(new_value) == str else str(new_value)


class TextType(StreamDataType):
    """Covers: TEXT, STRING, CLOB and CHARACTER LARGE OBJECT"""

    def __init__(self, **kwargs):
        super(TextType, self).__init__(**kwargs)

    def add_json_schema_entry(self, schema):
        super(TextType, self).add_json_schema_entry(schema)
        schema[self._column_name]['type'] = 'string'


class LimitedTextType(TextType):
    """Covers: CHAR, CHARACTER, VARCHAR, CHARACTER VARYING"""

    def __init__(self, **kwargs):
        self._limit = kwargs['limit']
        super(LimitedTextType, self).__init__(**kwargs)

    def add_json_schema_entry(self, schema):
        super(LimitedTextType, self).add_json_schema_entry(schema)
        schema[self._column_name]['maxLength'] = self._limit

    def to_json_representation(self):
        return super(LimitedTextType, self).to_json_representation().update({'limit', self._limit})

    def create_stream_sql(self):  # get column creation statement on SQL
        null_word = 'NOT NULL' if not self._is_nullable else 'NULL'
        return self._column_name + " " + self._data_type + "(" + str(self._limit) + ") " + null_word


class BooleanType(StreamDataType):
    """Covers: BOOL[EAN]"""

    def __init__(self, **kwargs):
        super(BooleanType, self).__init__(**kwargs)

    def add_json_schema_entry(self, schema):
        super(BooleanType, self).add_json_schema_entry(schema)
        schema[self._column_name]['type'] = 'boolean'

    def convert_value_into_sql(self, new_value):
        if type(new_value) == str:
            return '0' if new_value.lower() in ('f', 'false', '0') else '1'
        else:
            return '1' if bool(new_value) else '0'


class IntegerType(StreamDataType):
    """Covers: TINYINT, SMALLINT, INT[EGER], BIGINT, INTERVAL"""

    def __init__(self, **kwargs):
        super(IntegerType, self).__init__(**kwargs)

    def add_json_schema_entry(self, schema):
        super(IntegerType, self).add_json_schema_entry(schema)
        schema[self._column_name] = {
            "anyOf": [
                {"type": "string"},
                {"type": "integer"}
            ]
        }

    def convert_value_into_sql(self, new_value):
        return str(new_value) if type(new_value) == int else new_value


class FloatType(StreamDataType):
    """Covers: REAL, FLOAT and DOUBLE"""

    def __init__(self, **kwargs):
        super(FloatType, self).__init__(**kwargs)

    def add_json_schema_entry(self, schema):
        super(FloatType, self).add_json_schema_entry(schema)
        schema[self._column_name] = {
            "anyOf": [
                {"type": "string"},
                {"type": "number"}
            ]
        }

    def convert_value_into_sql(self, new_value):
        return str(new_value) if type(new_value) == float else new_value

EPOCH_DAY = date(1970, 1, 1)


class DateType(StreamDataType):
    """Covers: DATE"""

    def __init__(self, **kwargs):
        super(DateType, self).__init__(**kwargs)

    def add_json_schema_entry(self, schema):
        super(DateType, self).add_json_schema_entry(schema)
        schema[self._column_name] = {
            "anyOf": [
                {"type": "string", "format": 'date'},
                {"type": "integer"}
            ]
        }

    def convert_value_into_sql(self, new_value):
        if type(new_value) == int:  # days since the UNIX Epoch
            delta = timedelta(new_value)
            offset = EPOCH_DAY + delta
            return str(offset)
        else:
            return new_value

TIME_REGEX = "^([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])\.\d{3}([\+-]([01]\d|2[0-3]):[0-5]\d)?$"


class TimeType(StreamDataType):
    """Covers: TIME"""

    def __init__(self, **kwargs):
        super(TimeType, self).__init__(**kwargs)

    def add_json_schema_entry(self, schema):
        super(TimeType, self).add_json_schema_entry(schema)
        schema[self._column_name] = {
            "anyOf": [
                {"type": "string", "pattern": TIME_REGEX},
                {"type": "integer"}
            ]
        }

    def convert_value_into_sql(self, new_value):
        if type(new_value) == int:  # seconds since the beginning of the day
            m, s = divmod(new_value, 60)
            h, m = divmod(m, 60)
            return "%d:%02d:%02d" % (h, m, s)
        else:
            return new_value


class TimestampType(StreamDataType):
    """Covers: TIMESTAMP"""

    def __init__(self, **kwargs):
        super(TimestampType, self).__init__(**kwargs)

    def add_json_schema_entry(self, schema):
        super(TimestampType, self).add_json_schema_entry(schema)
        schema[self._column_name] = {
            "anyOf": [
                {"type": "string", "format": 'date-time'},
                {"type": "integer"}
            ]
        }

    def convert_value_into_sql(self, new_value):
        return datetime.fromtimestamp(new_value).isoformat() if type(new_value) == int else new_value
