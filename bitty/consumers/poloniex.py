import copy
import logging
import ujson
import aiohttp
import sys

from functools import partial
from concurrent.futures import CancelledError
from bitty.consumers.base import BaseConsumer
from bitty.consumers.wamp import WAMPClient


json_loads = ujson.loads  # pylint: disable=no-member


logger = logging.getLogger(__name__)


SUBSCRIBE_MSG = {
    'type': 'subscribe',
    'product_ids': [
        'BTC-USD',
        'ETH-USD',
        'ETH-BTC',
        'LTC-USD',
        'LTC-BTC'
   ]
}


class PoloniexConsumer(BaseConsumer):
    enable_heartbeat = False
    url = 'wss://api.poloniex.com'
    exchange_name = 'Poloniex'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wamp = WAMPClient(self.url)

    def process_message(self, messages, product_id=None):
        rv = []
        for msg in messages:
            if msg.get('type') == 'newTrade':
                rv.append({
                    'type': 'trade',
                    'price': msg.get('data').get('rate'),
                    'product_id': product_id,
                    'side': msg.get('data').get('type'),
                    'time': msg.get('data').get('date'),
                    'sequence': 0,
                    'size': msg.get('data').get('amount'),
                    'trade_id': msg.get('data').get('tradeID')
                })
        if rv:
            return rv
        return None

    def make_subscribe_payload(self):
        subscribe_msg = copy.deepcopy(SUBSCRIBE_MSG)
        subscribe_msg['product_ids'] = self.product_ids
        return subscribe_msg


    async def on_welcome(self):
        logger.info('%s wamp start on welcome', self.consumer_id)
        await self.spawn_keepalive()

    async def consume(self):
        if self.terminated:
            logger.info('%s Termination requested. not consuming',
                        self.consumer_id)
            return

        session = aiohttp.ClientSession()

        for product_id in self.product_ids:
            self.wamp.subscribe(
                partial(self.on_message, product_id=product_id),
                product_id
            )

        self.wamp.on_welcome(self.on_welcome)

        try:
            logger.info('%s starting wamp session', self.consumer_id)
            await self.wamp.start(session)
        except CancelledError:
            raise
        except:  # pylint:disable=bare-except
            logger.exception('Consume failed')
        finally:
            session.close()

    async def kill(self):
        logger.warning('requested consumer kill. Terminating app')
        if self.terminated:
            return

        self.terminated = True
        if self.wamp:
            await self.wamp.stop()

    async def reconnect(self, *, kill_keepalive=True, kill_consumer=True):
        if kill_keepalive and self.keepalive:
            self.keepalive.cancel()
        if kill_keepalive and self.trade_keepalive:
            self.trade_keepalive.cancel()

        if kill_consumer and self.consumer:
            self.consumer.remove_done_callback(self.on_consume_end)
            self.consumer.cancel()

        logger.info('reconnecting.... closing ws')
        if self.wamp:
            await self.wamp.stop()
        self.spawn_consumer()
