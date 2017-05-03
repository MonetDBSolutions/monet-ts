from jsonschema import Draft4Validator, FormatChecker

TIMED_FLUSH_IDENTIFIER = "time"
TUPLE_FLUSH_IDENTIFIER = "tuple"
AUTO_FLUSH_IDENTIFIER = "auto"

TEXT_INPUT = "clob"
UNBOUNDED_TEXT_INPUTS = [TEXT_INPUT, "text", "string"]
BOUNDED_TEXT_INPUTS = ["char", "varchar", "character"]

BOOLEAN_INPUT = "bool"
BOOLEAN_INPUTS = [BOOLEAN_INPUT, "boolean"]

INTERVAL_TYPES_INTERNAL = ["sec_interval", "month_interval"]
INTERVAL_INPUTS = ["interval second", "interval minute", "interval hour", "interval day", "interval month",
                   "interval year", "interval year to month", "interval day to hour", "interval day to minute",
                   "interval day to second", "interval hour to minute", "interval hour to second",
                   "interval minute to second"]

INTEGER_INPUT = "int"
INTEGER_INPUTS = ["tinyint", "smallint", INTEGER_INPUT, "bigint", "integer"] + INTERVAL_INPUTS

FLOATING_POINT_PRECISION_INPUT = "real"
FLOATING_POINT_PRECISION_INPUTS = [FLOATING_POINT_PRECISION_INPUT, "double", "float"]

DATE_INPUTS = ["date"]

TIME_WITH_TIMEZONE_TYPE_INTERNAL = "timetz"
TIME_WITH_TIMEZONE_TYPE_EXTERNAL = "time with time zone"
TIME_INPUTS = ["time", TIME_WITH_TIMEZONE_TYPE_INTERNAL, TIME_WITH_TIMEZONE_TYPE_EXTERNAL]

TIMESTAMP_WITH_TIMEZONE_TYPE_INTERNAL = "timestamptz"
TIMESTAMP_WITH_TIMEZONE_TYPE_EXTERNAL = "timestamp with time zone"
TIMESTAMP_INPUTS = ["timestamp", TIMESTAMP_WITH_TIMEZONE_TYPE_INTERNAL, TIMESTAMP_WITH_TIMEZONE_TYPE_EXTERNAL]

CREATE_STREAMS_SCHEMA = Draft4Validator({
    "title": "JSON schema to create a stream",
    "description": "Validate the inserted properties",
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
        "schema": {"type": "string", "pattern": "[a-zA-Z0-9]+"},
        "stream": {"type": "string", "pattern": "[a-zA-Z0-9]+"},
        "flushing": {
            "type": "object",
            "oneOf": [{
                "properties": {
                    "base": {"type": "string", "enum": [TIMED_FLUSH_IDENTIFIER]},
                    "interval": {"type": "integer", "minimum": 1},
                    "unit": {"type": "string", "enum": ["s", "m", "h"]}
                },
                "required": ["base", "interval", "unit"],
                "additionalProperties": False
            }, {
                "properties": {
                    "base": {"type": "string", "enum": [TUPLE_FLUSH_IDENTIFIER]},
                    "interval": {"type": "integer", "minimum": 1}
                },
                "required": ["base", "interval"],
                "additionalProperties": False
            }, {
                "properties": {
                    "base": {"type": "string", "enum": [AUTO_FLUSH_IDENTIFIER]},
                },
                "required": ["base"],
                "additionalProperties": False
            }]
        },
        "columns": {
            "type": "array",
            "minItems": 1,
            "additionalItems": False,
            "items": {
                "type": "object",
                "anyOf": [{
                    "properties": {
                        "name": {"type": "string", "pattern": "[a-zA-Z0-9]+"},
                        "type": {"type": "string", "enum": UNBOUNDED_TEXT_INPUTS},
                        "nullable": {"type": "boolean", "default": True}
                    },
                    "required": ["name", "type"],
                    "additionalProperties": False
                }, {
                    "properties": {
                        "name": {"type": "string", "pattern": "[a-zA-Z0-9]+"},
                        "type": {"type": "string", "enum": BOUNDED_TEXT_INPUTS},
                        "nullable": {"type": "boolean", "default": True},
                        "limit": {"type": "integer", "minimum": 1}
                    },
                    "required": ["name", "type", "limit"],
                    "additionalProperties": False
                }, {
                    "properties": {
                        "name": {"type": "string", "pattern": "[a-zA-Z0-9]+"},
                        "type": {"type": "string", "enum": BOOLEAN_INPUTS},
                        "nullable": {"type": "boolean", "default": True}
                    },
                    "required": ["name", "type"],
                    "additionalProperties": False
                }, {
                    "properties": {
                        "name": {"type": "string", "pattern": "[a-zA-Z0-9]+"},
                        "type": {"type": "string", "enum": INTEGER_INPUTS},
                        "nullable": {"type": "boolean", "default": True}
                    },
                    "required": ["name", "type"],
                    "additionalProperties": False
                }, {
                    "properties": {
                        "name": {"type": "string", "pattern": "[a-zA-Z0-9]+"},
                        "type": {"type": "string", "enum": FLOATING_POINT_PRECISION_INPUTS},
                        "nullable": {"type": "boolean", "default": True}
                    },
                    "required": ["name", "type"],
                    "additionalProperties": False
                }, {
                    "properties": {
                        "name": {"type": "string", "pattern": "[a-zA-Z0-9]+"},
                        "type": {"type": "string", "enum": DATE_INPUTS},
                        "nullable": {"type": "boolean", "default": True}
                    },
                    "required": ["name", "type"],
                    "additionalProperties": False
                }, {
                    "properties": {
                        "name": {"type": "string", "pattern": "[a-zA-Z0-9]+"},
                        "type": {"type": "string", "enum": TIME_INPUTS},
                        "nullable": {"type": "boolean", "default": True}
                    },
                    "required": ["name", "type"],
                    "additionalProperties": False
                }, {
                    "properties": {
                        "name": {"type": "string", "pattern": "[a-zA-Z0-9]+"},
                        "type": {"type": "string", "enum": TIMESTAMP_INPUTS},
                        "nullable": {"type": "boolean", "default": True}
                    },
                    "required": ["name", "type"],
                    "additionalProperties": False
                }]
            }
        },
        "tags": {
            "type": "array",
            "minItems": 1,
            "uniqueItems": True,
            "items": {
                "type": "string"
            }
        }
    },
    "required": ["schema", "stream", "flushing", "columns"],
    "additionalProperties": False
}, format_checker=FormatChecker())

DELETE_STREAMS_SCHEMA = Draft4Validator({
    "title": "JSON schema to delete a stream",
    "description": "Just send the schema and the stream",
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
        "schema": {"type": "string", "pattern": "[a-zA-Z0-9]+"},
        "stream": {"type": "string", "pattern": "[a-zA-Z0-9]+"}
    },
    "required": ["schema", "stream"],
    "additionalProperties": False
}, format_checker=FormatChecker())

INSERT_DATA_SCHEMA = Draft4Validator({
    "title": "Insert data into streams",
    "description": "Validate the inserted properties",
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "array",
    "minItems": 1,
    "items": {
        "type": "object",
        "properties": {
            "schema": {"type": "string", "pattern": "[a-zA-Z0-9]+"},
            "stream": {"type": "string", "pattern": "[a-zA-Z0-9]+"},
            "values": {
                "type": "array",
                "minItems": 1,
                "uniqueItems": False,
                "items": {"type": "object"}
            }
        },
        "required": ["schema", "stream", "values"]
    }
}, format_checker=FormatChecker())
