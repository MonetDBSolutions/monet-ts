from abc import ABCMeta, abstractmethod
from typing import Any, Dict


class StreamDataType(object):
    """MonetDB's data types for validation base class"""
    __metaclass__ = ABCMeta

    def __init__(self, **kwargs):
        self._column_name = kwargs['name']  # name of the column
        self._data_type = kwargs['type']  # SQL name of the type
        self._is_nullable = kwargs.get('nullable', True)  # boolean
        self._is_tag = kwargs.get('tag', False)  # boolean

    def column_name(self) -> str:  # the column name, just that
        return self._column_name

    def is_nullable(self) -> bool:  # check if the column is nullable or not
        return self._is_nullable

    def is_tag(self) -> bool:  # check if the column is a tag or not
        return self._is_tag

    def add_json_schema_entry(self, schema) -> None:  # add the entry for the stream's corresponding json schema
        schema[self._column_name] = {}

    # get a json representation of the data type while checking the stream's info
    def to_json_representation(self) -> Dict[str, Any]:
        return {'name': self._column_name, 'type': self._data_type, 'nullable': self._is_nullable, 'tag': self._is_tag}

    def create_stream_sql(self) -> str:  # get column creation statement on SQL
        null_word = 'NOT NULL' if not self._is_nullable else 'NULL'
        return "\"%s\" %s %s" % (self._column_name, self._data_type, null_word)

    @abstractmethod
    def convert_value_into_sql(self, new_value: Any) -> str:
        pass


class TextType(StreamDataType):
    """Covers: TEXT, STRING, CLOB and CHARACTER LARGE OBJECT"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_json_schema_entry(self, schema) -> None:
        super().add_json_schema_entry(schema)
        schema[self._column_name]['type'] = 'string'

    def convert_value_into_sql(self, new_value) -> str:
        return "'" + new_value + "'"


class LimitedTextType(TextType):
    """Covers: CHAR, CHARACTER, VARCHAR, CHARACTER VARYING"""

    def __init__(self, **kwargs):
        self._limit = kwargs['limit']
        super().__init__(**kwargs)

    def add_json_schema_entry(self, schema) -> None:
        super().add_json_schema_entry(schema)
        schema[self._column_name]['maxLength'] = self._limit

    def to_json_representation(self) -> Dict[str, Any]:
        previous_dict = super(LimitedTextType, self).to_json_representation()
        previous_dict['limit'] = self._limit
        return previous_dict

    def create_stream_sql(self) -> str:  # get column creation statement on SQL
        null_word = 'NOT NULL' if not self._is_nullable else 'NULL'
        return self._column_name + " " + self._data_type + "(" + str(self._limit) + ") " + null_word


class BooleanType(StreamDataType):
    """Covers: BOOL[EAN]"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_json_schema_entry(self, schema) -> None:
        super().add_json_schema_entry(schema)
        schema[self._column_name]['type'] = 'boolean'

    def convert_value_into_sql(self, new_value) -> str:
        return '1' if bool(new_value) else '0'


class IntegerType(StreamDataType):
    """Covers: TINYINT, SMALLINT, INT[EGER], BIGINT, INTERVAL"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_json_schema_entry(self, schema) -> None:
        super().add_json_schema_entry(schema)
        schema[self._column_name] = {"type": "integer"}

    def convert_value_into_sql(self, new_value) -> str:
        return str(new_value)


class FloatType(StreamDataType):
    """Covers: REAL, FLOAT and DOUBLE"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_json_schema_entry(self, schema) -> None:
        super().add_json_schema_entry(schema)
        schema[self._column_name] = {"type": "number"}

    def convert_value_into_sql(self, new_value) -> str:
        return str(new_value)


class DateType(StreamDataType):
    """Covers: DATE"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_json_schema_entry(self, schema) -> None:
        super().add_json_schema_entry(schema)
        schema[self._column_name] = {"type": "string", "format": 'date'}

    def convert_value_into_sql(self, new_value) -> str:
        return new_value


TIME_REGEX = "^([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])\.\d{3}([\+-]([01]\d|2[0-3]):[0-5]\d)?$"


class TimeType(StreamDataType):
    """Covers: TIME"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_json_schema_entry(self, schema) -> None:
        super().add_json_schema_entry(schema)
        schema[self._column_name] = {"type": "string", "pattern": TIME_REGEX}

    def convert_value_into_sql(self, new_value) -> str:
        return new_value


class TimestampType(StreamDataType):
    """Covers: TIMESTAMP"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_json_schema_entry(self, schema) -> None:
        super().add_json_schema_entry(schema)
        schema[self._column_name] = {
            "anyOf": [
                {"type": "string", "format": 'date-time'},
                {"type": "integer"}
            ]
        }

    def convert_value_into_sql(self, new_value) -> str:  # the glamorous UNIX timestamp!
        return 'sys.epoch(' + str(new_value) + ')' if type(new_value) == int else "'" + new_value + "'"
