import asyncio
import logging

from hbmqtt.client import MQTTClient, ClientException
from hbmqtt.mqtt.constants import QOS_2
from jsonschema import ValidationError
from ingest.inputs import influxdb
from ingest.monetdb.naming import THREAD_POOL
from ingest.streams.context import get_streams_context

logger = logging.getLogger('MonetDBTS ' + __name__)


async def mqttclient_coro():
    C = MQTTClient()
    await C.connect('mqtt://localhost:1883/')
    await C.subscribe([('/influxdb', QOS_2), ])
    try:
        while True:
            message = await C.deliver_message()
            packet = message.publish_packet

            try:  # TODO check UTF-8 strings?
                schema, stream, tags, values = influxdb.parse_influxdb_line(packet.payload.data.decode("utf-8"))
            except BaseException as ex:
                await C.publish('answer', ex.__str__().encode('utf-8'), qos=QOS_2)
                continue

            try:  # check if stream exists, if not return 404
                stream = get_streams_context().get_existing_stream(schema, stream)
            except BaseException as ex:
                await C.publish('answer', ex.__str__().encode('utf-8'), qos=QOS_2)
                continue

            try:  # validate and insert data, if not return 400
                await asyncio.wrap_future(THREAD_POOL.submit(stream.insert_json, [values]))
            except (ValidationError, BaseException) as ex:
                await C.publish('answer', ex.__str__().encode('utf-8'), qos=QOS_2)
                continue
    except ClientException:
        await C.unsubscribe(['/influxdb'])
        await C.disconnect()
