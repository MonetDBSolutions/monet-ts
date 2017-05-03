import asyncio

from hbmqtt.client import MQTTClient, ClientException
from hbmqtt.mqtt.constants import QOS_2

from ingest.monetdb.naming import THREAD_POOL
from ingest.streams.context import get_streams_context
from ingest.streams.streamexception import StreamException
from ingest.tsinfluxline import influxdbparser
from ingest.tsinfluxline.linereader import read_chunk_lines, CHUNK_SIZE


async def mqttclient_coro():
    client = MQTTClient()
    await client.connect('mqtt://localhost:1883/')
    await client.subscribe([('/influxdb', QOS_2), ])
    try:
        while True:
            base_tuple_counter = 0
            errors = []
            message = await client.deliver_message()
            packet = message.publish_packet

            for lines in read_chunk_lines(packet.payload.data.decode("utf-8"), CHUNK_SIZE):
                try:
                    grouped_streams = influxdbparser.parse_influxdb_line(lines, base_tuple_counter)
                    try:  # TODO check UTF-8 strings?
                        for key, values in grouped_streams.items():
                            stream = get_streams_context().get_existing_metric(key)
                            await asyncio.wrap_future(THREAD_POOL.submit(stream.insert_values, values,
                                                                         base_tuple_counter,
                                                                         'convert_value_into_sql_from_influxdb'))
                    except StreamException as ex:
                        errors.append(ex.args[0]['message'])
                except StreamException as ex:
                    errors.append(ex.args[0]['message'])

                base_tuple_counter += CHUNK_SIZE

            if len(errors) > 0:
                await client.publish('answer', errors.__str__().encode('utf-8'), qos=QOS_2)
            else:
                await client.publish('answer', 'OK'.encode('utf-8'), qos=QOS_2)

    except ClientException:
        await client.unsubscribe(['/influxdb'])
        await client.disconnect()
