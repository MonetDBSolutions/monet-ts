from ingest.streams.guardianexception import GuardianException
from ingest.tsinfluxline.influxdblineparser import add_influxdb_lines, discovery_influxdb_lines_fast, \
    discovery_influxdb_lines_slow
from tshttp.tshandlers import TSLineHandler


class InfluxDBInput(TSLineHandler):
    """Ingest lines under the InfluxDB protocol"""

    async def post(self):
        try:
            await add_influxdb_lines(self.request.body.decode("utf-8"))
            self.set_status(201)
        except GuardianException as ex:
            self.write_guardian_exception(ex)


class InfluxDBDiscovery(TSLineHandler):
    """Ingest a lines under the InfluxDB protocol or create the streams if they don't exist"""

    async def post(self):
        try:
            await discovery_influxdb_lines_fast(self.request.body.decode("utf-8"))
            self.set_status(201)
        except GuardianException as ex:
            self.write_guardian_exception(ex)


class InfluxDBDiscoverySlow(TSLineHandler):
    """A slower version of the above method"""

    async def post(self):
        try:
            await discovery_influxdb_lines_slow(self.request.body.decode("utf-8"))
            self.set_status(201)
        except GuardianException as ex:
            self.write_guardian_exception(ex)
