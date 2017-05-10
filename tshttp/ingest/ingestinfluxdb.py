from ingest.tsinfluxline.influxdblineparser import add_influxdb_lines, discovery_influxdb_lines
from tshttp.tslinehandler import TSBaseLineHandler


class InfluxDBInput(TSBaseLineHandler):
    """Ingest lines under the InfluxDB protocol"""

    async def post(self):
        errors = await add_influxdb_lines(self.request.body.decode("utf-8"))

        if len(errors) > 0:
            self.write_error(400, **{'messages': errors})
        else:
            self.set_status(201)


class InfluxDBDiscovery(TSBaseLineHandler):
    """Ingest a lines under the InfluxDB protocol or create the streams if they don't exist"""

    async def post(self):
        errors = await discovery_influxdb_lines(self.request.body.decode("utf-8"))

        if len(errors) > 0:
            self.write_error(400, **{'messages': errors})
        else:
            self.set_status(201)
