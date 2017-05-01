from hbmqtt.broker import Broker


async def ts_broker_start(broker_port):
    # TODO I can't change the default bind port, try it if you can
    ts_broker_config = {
        'listeners': {
            'default': {
                'type': 'tcp',
                'bind': '0.0.0.0:1883'
            }
        },
        'sys_interval': 0,
        'auth': {
            'allow-anonymous': True
        }
    }

    ts_broker = Broker(ts_broker_config)
    await ts_broker.start()
