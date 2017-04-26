import asyncio
import logging
import tornado.web, tornado.httpserver, tornado.ioloop, tornado.platform

from tshttp import guardian_application
from tsmqtt.mqttbroker import broker_coro
from tsmqtt.mqttclient import mqttclient_coro

logger = logging.getLogger('MonetDBTS ' + __name__)


def init_servers(tornado_port):
    tornado.platform.asyncio.AsyncIOMainLoop().install()
    guardian_application.listen(tornado_port)
    output = "Monet Time Stream server listening on port: " + str(tornado_port)
    logger.info(output)
    print(output)

    asyncio.get_event_loop().run_until_complete(broker_coro())  # Run the broker first :)
    asyncio.get_event_loop().run_until_complete(mqttclient_coro())

    asyncio.get_event_loop().run_forever()
