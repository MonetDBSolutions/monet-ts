import asyncio
import logging
import tornado.web, tornado.httpserver, tornado.ioloop, tornado.platform

from tshttp import guardian_application
from tsmqtt.mqttbroker import ts_broker_start
from tsmqtt.mqttclient import mqttclient_coro

logger = logging.getLogger('MonetDBTS ' + __name__)


def init_servers(tornado_port, broker_port):
    tornado.platform.asyncio.AsyncIOMainLoop().install()
    guardian_application.listen(tornado_port)
    http_log = "Monet Time Stream HTTP server listening on port: " + str(tornado_port)
    logger.info(http_log)
    print(http_log)

    asyncio.get_event_loop().run_until_complete(ts_broker_start(broker_port))  # Run the TS_BROKER first :)
    mqtt_broker_log = "MQTT broker listening on port 1833"
    logger.info(mqtt_broker_log)
    print(mqtt_broker_log)

    asyncio.get_event_loop().run_until_complete(mqttclient_coro())
    mqtt_client_log = "MQTT client connected"
    logger.info(mqtt_client_log)
    print(mqtt_client_log)

    asyncio.get_event_loop().run_forever()
