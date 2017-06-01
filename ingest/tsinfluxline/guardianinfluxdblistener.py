from typing import Dict, Any, List

from ingest.monetdb.naming import METRIC_SEPARATOR, TIMESTAMP_COLUMN_NAME, DATABASE_SCHEMA, get_metric_name
from ingest.tsinfluxline.grammar.influxdbParser import influxdbParser
from ingest.tsinfluxline.grammar.influxdbListener import influxdbListener


class GuardianInfluxDBListener(influxdbListener):

    def __init__(self):
        super().__init__()
        self._grouped_streams = {}
        self._found_errors = []
        # the lines bellow are to be used during the parsing
        self._line_number = 0
        self._tag_number = 0
        self._value_number = 0
        self._current_schema = ''
        self._current_stream = ''
        self._current_values = {}
        self._there_is_error = None

    def get_parsed_values(self) -> List[Dict[str, Any]]:
        return list(self._grouped_streams.values())

    def get_found_errors(self) -> List[str]:
        return self._found_errors

    def enterLine(self, ctx: influxdbParser.LineContext) -> None:
        self._line_number += 1
        self._tag_number = 0
        self._value_number = 0
        self._current_schema = ''
        self._current_stream = ''
        self._current_values = {}
        self._there_is_error = None

    def exitLine(self, ctx: influxdbParser.LineContext) -> None:
        if self._there_is_error is None:  # append only when there are no errors
            metric_name = get_metric_name(self._current_schema, self._current_stream)
            if metric_name not in self._grouped_streams:
                self._grouped_streams[metric_name] = {'schema': self._current_schema, 'stream': self._current_stream,
                                                      'values': [self._current_values]}
            else:
                self._grouped_streams[metric_name]['values'].append(self._current_values)
        else:
            self._found_errors.append(self._there_is_error)

    def enterMetric(self, ctx: influxdbParser.MetricContext) -> None:
        metric = ctx.INFLUXWORD()
        if metric is None:
            self._there_is_error = "The metric name is missing at line %d!" % self._line_number
            return

        text = metric.getText()
        split = text.split(METRIC_SEPARATOR)
        if len(split) == 1:  # by default we will set the line to timetrails schema
            self._current_schema = DATABASE_SCHEMA
            self._current_stream = split[0]
        else:
            self._current_schema = split[0]
            self._current_stream = text[(len(split[0]) + 1):]

    def enterTtype(self, ctx: influxdbParser.TtypeContext) -> None:
        if self._there_is_error is not None:
            return

        self._tag_number += 1
        next_tkey = ctx.INFLUXWORD(0)
        next_tvalue = ctx.INFLUXWORD(1)

        if next_tkey is None:
            self._there_is_error = "The tag name at column %d is missing at line %d!" % \
                                   (self._tag_number, self._line_number)
            return
        elif next_tvalue is None:
            self._there_is_error = "The value at column %d is missing at line %d!" % \
                                   (self._tag_number, self._line_number)
            return

        next_tkey = next_tkey.getText()

        if next_tkey in self._current_values:
            self._there_is_error = "The tag %s is duplicated at line %d!" % (next_tkey, self._line_number)
            return

        self._current_values[next_tkey] = next_tvalue.getText()

    def enterVtype(self, ctx: influxdbParser.VtypeContext) -> None:
        if self._there_is_error is not None:
            return

        self._value_number += 1

        next_vkey = ctx.INFLUXWORD(0)
        next_vvalue = ctx.INFLUXWORD(1)
        next_vstring = ctx.INFLUXSTRING()

        if next_vkey is None:
            self._there_is_error = "The column name at column %d at line %d is missing!" % \
                                   (self._value_number, self._line_number)
            return

        next_vkey = next_vkey.getText()

        if next_vkey in self._current_values:
            self._there_is_error = "The column %s is duplicated at line %d!" % (next_vkey, self._line_number)
            return

        if next_vvalue is not None:
            next_vvalue = next_vvalue.getText()
            if next_vvalue in ('t', 'T', 'true', 'True', 'TRUE'):
                self._current_values[next_vkey] = True
                return
            if next_vvalue in ('f', 'F', 'false', 'False', 'FALSE'):
                self._current_values[next_vkey] = False
                return

            if next_vvalue.endswith('i'):
                try:
                    self._current_values[next_vkey] = int(next_vvalue[:-1])
                    return
                except:
                    pass
            try:
                self._current_values[next_vkey] = float(next_vvalue)
                return
            except:
                pass
        elif next_vstring is not None:
            self._current_values[next_vkey] = next_vstring.getText()[1:-1]
            return

        self._there_is_error = "The value at column %d at line %d is not in a valid InfluxDB format!" % \
                               (self._value_number, self._line_number)

    def enterTimestamp(self, ctx: influxdbParser.TimestampContext) -> None:
        if self._there_is_error is not None:
            return

        next_timestamp = ctx.INFLUXWORD()
        try:
            self._current_values[TIMESTAMP_COLUMN_NAME] = int(next_timestamp.getText()[:-3])
        except:
            self._there_is_error = "The timestamp at line %d is not in the valid format!" % self._line_number
            return
