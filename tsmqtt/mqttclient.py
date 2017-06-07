from hbmqtt.client import MQTTClient, ClientException
from hbmqtt.mqtt.constants import QOS_2

from ingest.streams.guardianexception import GuardianException, MQTT_PROTOCOL_VIOLATION
from ingest.tsinfluxline.influxdblineparser import add_influxdb_lines, discovery_influxdb_lines_fast
from ingest.tsjson.jsonparser import add_json_lines

JSON_TOPIC = '/json'
INFLUXDB_TOPIC = '/influxdb'
DISCOVERY_TOPIC = '/discovery'

SUBSCRIPTION_TOPICS = [JSON_TOPIC, INFLUXDB_TOPIC, DISCOVERY_TOPIC]
QOS_VALUES = list(map(lambda x: (x, QOS_2), SUBSCRIPTION_TOPICS))

ANSWER_TOPIC = '/answer'

async def mqttclient_coro():
    client = MQTTClient()
    await client.connect('mqtt://localhost:1883/')
    await client.subscribe(QOS_VALUES)
    try:
        while True:
            message = await client.deliver_message()
            packet = message.publish_packet
            topic_name = packet.variable_header.topic_name
            decoded_packet_data = packet.payload.data.decode('utf-8')

            try:
                if topic_name == INFLUXDB_TOPIC:
                    await add_influxdb_lines(decoded_packet_data)
                elif topic_name == DISCOVERY_TOPIC:
                    await discovery_influxdb_lines_fast(decoded_packet_data)
                elif topic_name == JSON_TOPIC:
                    await add_json_lines(decoded_packet_data)
                else:
                    raise GuardianException(where=MQTT_PROTOCOL_VIOLATION, message='Unknown topic %s!' % topic_name)

                await client.publish(ANSWER_TOPIC, 'k'.encode('utf-8'), qos=QOS_2)
            except GuardianException as ex:
                if isinstance(ex.message, (list, tuple)):
                    printing = '{"messages":["' + '","'.join(ex.message) + '"]}'
                else:
                    printing = '{"message":"' + ex.message + '"}'
                await client.publish(ANSWER_TOPIC, printing.encode('utf-8'), qos=QOS_2)
    except ClientException as ex:
        print("An error occurred in the Guardian MQTT client: %s", ex.__str__())
        await client.unsubscribe(SUBSCRIPTION_TOPICS)
        await client.disconnect()
