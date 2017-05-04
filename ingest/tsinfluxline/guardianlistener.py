from ingest.monetdb.naming import TIMETRAILS_SCHEMA, METRIC_SEPARATOR, TIMESTAMP_COLUMN_NAME
from ingest.tsinfluxline.grammar.influxdbParser import influxdbParser
from ingest.tsinfluxline.grammar.influxdbListener import influxdbListener


class GuardianListener(influxdbListener):

    def __init__(self, base_tuple_counter):
        super().__init__()
        self._grouped_streams = {}
        self._line_number = base_tuple_counter
        self._value_number = 0

    def get_grouped_streams(self):
        return self._grouped_streams

    def enterLine(self, ctx: influxdbParser.LineContext):
        self._current_stream = {'metric_name': '', 'tags': [], 'values': {}}
        self._line_number += 1
        self._value_number = 0

    def exitLine(self, ctx: influxdbParser.LineContext):
        metric_name = self._current_stream['metric_name']
        if metric_name not in self._grouped_streams:
            self._grouped_streams[metric_name] = {'tags': self._current_stream['tags'],
                                                  'values': [self._current_stream['values']]}
        else:
            self._grouped_streams[metric_name]['values'].append(self._current_stream['values'])

    def enterMetric(self, ctx:influxdbParser.MetricContext):
        full_metric = ctx.INFLUXWORD()
        text = full_metric.getText()
        split = text.split('.')

        if len(split) == 1:  # by default we will set the line to timetrails
            self._current_stream['metric_name'] = TIMETRAILS_SCHEMA + METRIC_SEPARATOR + split[0]
        else:
            self._current_stream['metric_name'] = split[0] + METRIC_SEPARATOR + text[(len(split[0]) + 1):]

    def enterTtype(self, ctx:influxdbParser.TtypeContext):
        next_tkey = ctx.INFLUXWORD(0)
        next_tvalue = ctx.INFLUXWORD(1)

        self._current_stream['tags'].append(next_tkey.getText())
        self._current_stream['values'][next_tkey.getText()] = next_tvalue.getText()

    def enterVtype(self, ctx:influxdbParser.VtypeContext):
        self._value_number += 1

        next_vkey = ctx.INFLUXWORD(0).getText()
        next_vvalue = ctx.INFLUXWORD(1)
        next_vstring = ctx.INFLUXSTRING()

        if next_vvalue is not None:
            next_vvalue = next_vvalue.getText()
            if next_vvalue in ('t', 'T', 'true', 'True', 'TRUE'):
                self._current_stream['values'][next_vkey] = True
                return
            if next_vvalue in ('f', 'F', 'false', 'False', 'FALSE'):
                self._current_stream['values'][next_vkey] = False
                return

            if next_vvalue.endswith('i'):
                try:
                    self._current_stream['values'][next_vkey] = int(next_vvalue[:-1])
                    return
                except:
                    pass
            try:
                self._current_stream['values'][next_vkey] = float(next_vvalue)
                return
            except:
                pass
        elif next_vstring is not None:
            self._current_stream['values'][next_vkey] = next_vstring.getText()[1:-1]
            return

        raise BaseException("The column %d at line %d is not a valid InfluxDB format" % (self._line_number,
                                                                                         self._value_number))

    def enterTimestamp(self, ctx:influxdbParser.TimestampContext):
        next_timestamp = ctx.INFLUXWORD()
        try:
            self._current_stream['values'][TIMESTAMP_COLUMN_NAME] = int(next_timestamp.getText())
        except:
            raise BaseException("The timestamp at line %d is not in a valid format" % self._line_number)
