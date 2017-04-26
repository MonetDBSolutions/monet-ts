from hbmqtt.broker import Broker

CONFIG = {
    'listeners': {
        'default': {
            'type': 'tcp',
            'bind': '0.0.0.0:1883',
        }
    },
    'sys_interval': 0,
    'auth': {
        'allow-anonymous': True
    }
}

broker = Broker(CONFIG)


async def broker_coro():
    await broker.start()
