from abc import ABCMeta, abstractmethod
from typing import Any, Dict

from ingest.tsinfluxline.influxdbparser import InfluxDBLineException, STRING_REGEX, BOOLEAN_REGEX, INTEGER_REGEX, \
    FLOATING_POINT_REGEX, TIMESTAMP_REGEX, TAG_REGEX


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
        return "%s %s %s" % (self._column_name, self._data_type, null_word)

    @abstractmethod
    def convert_value_into_sql_from_json(self, new_value: Any) -> str:
        pass

    @abstractmethod
    def convert_value_into_sql_from_influxdb(self, new_value) -> str:
        pass

    @abstractmethod
    def validate_influxdb_entry(self, new_value) -> None:
        pass


class TextType(StreamDataType):
    """Covers: TEXT, STRING, CLOB and CHARACTER LARGE OBJECT"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_json_schema_entry(self, schema) -> None:
        super().add_json_schema_entry(schema)
        schema[self._column_name]['type'] = 'string'

    def convert_value_into_sql_from_json(self, new_value) -> str:
        return "'" + new_value + "'"

    def convert_value_into_sql_from_influxdb(self, new_value) -> str:
        if not self._is_tag:
            new_value = new_value[1:-1]
        return "'" + new_value + "'"

    def validate_influxdb_entry(self, new_value) -> None:
        if self._is_tag and TAG_REGEX.match(new_value) is None:
            raise InfluxDBLineException("The string %s is not a valid InfluxDB tag!")
        elif STRING_REGEX.match(new_value) is None:
            raise InfluxDBLineException("The string %s is not a valid InfluxDB string!")


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

    def convert_value_into_sql_from_json(self, new_value) -> str:
        return '1' if bool(new_value) else '0'

    def convert_value_into_sql_from_influxdb(self, new_value) -> str:
        return '0' if new_value.lower() in ('f', 'false', '0') else '1'

    def validate_influxdb_entry(self, new_value) -> None:
        if BOOLEAN_REGEX.match(new_value) is None:
            raise InfluxDBLineException("The string %s is not a valid InfluxDB boolean!")


class IntegerType(StreamDataType):
    """Covers: TINYINT, SMALLINT, INT[EGER], BIGINT, INTERVAL"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_json_schema_entry(self, schema) -> None:
        super().add_json_schema_entry(schema)
        schema[self._column_name] = {"type": "integer"}

    def convert_value_into_sql_from_json(self, new_value) -> str:
        return str(new_value)

    def convert_value_into_sql_from_influxdb(self, new_value) -> str:
        return new_value[:-1]

    def validate_influxdb_entry(self, new_value) -> None:
        if INTEGER_REGEX.match(new_value) is None:
            raise InfluxDBLineException("The string %s is not a valid InfluxDB integer!")


class FloatType(StreamDataType):
    """Covers: REAL, FLOAT and DOUBLE"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_json_schema_entry(self, schema) -> None:
        super().add_json_schema_entry(schema)
        schema[self._column_name] = {"type": "number"}

    def convert_value_into_sql_from_json(self, new_value) -> str:
        return str(new_value)

    def convert_value_into_sql_from_influxdb(self, new_value) -> str:
        return new_value

    def validate_influxdb_entry(self, new_value) -> None:
        if FLOATING_POINT_REGEX.match(new_value) is None:
            raise InfluxDBLineException("The string %s is not a valid InfluxDB floating-point!")


class DateType(StreamDataType):
    """Covers: DATE"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_json_schema_entry(self, schema) -> None:
        super().add_json_schema_entry(schema)
        schema[self._column_name] = {"type": "string", "format": 'date'}

    def convert_value_into_sql_from_json(self, new_value) -> str:
        return new_value

    def convert_value_into_sql_from_influxdb(self, new_value) -> str:
        return new_value

    def validate_influxdb_entry(self, new_value)-> None:
        raise InfluxDBLineException("Dates are not supported by the InfluxDB line protocol :(")

TIME_REGEX = "^([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])\.\d{3}([\+-]([01]\d|2[0-3]):[0-5]\d)?$"


class TimeType(StreamDataType):
    """Covers: TIME"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def add_json_schema_entry(self, schema) -> None:
        super().add_json_schema_entry(schema)
        schema[self._column_name] = {"type": "string", "pattern": TIME_REGEX}

    def convert_value_into_sql_from_json(self, new_value) -> str:
        return new_value

    def convert_value_into_sql_from_influxdb(self, new_value) -> str:
        return new_value

    def validate_influxdb_entry(self, new_value) -> None:
        raise InfluxDBLineException("Times are not supported by the InfluxDB line protocol :(")


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

    def convert_value_into_sql_from_json(self, new_value) -> str:  # the glamorous UNIX timestamp!
        return 'sys.epoch(' + str(new_value) + ')' if type(new_value) == int else "'" + new_value + "'"

    def convert_value_into_sql_from_influxdb(self, new_value) -> str:
        return 'sys.epoch(' + str(new_value) + ')' if type(new_value) == int else "'" + new_value + "'"

    def validate_influxdb_entry(self, new_value) -> None:
        if TIMESTAMP_REGEX.match(new_value) is None:
            raise InfluxDBLineException("The string %s is not a valid InfluxDB timestamp!")
