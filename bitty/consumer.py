import copy
import ujson
import asyncio
import aiohttp
import logging
import time
import sys

from concurrent.futures import CancelledError

from bitty import models, loggers
from bitty.consumers.gdax import GdaxConsumer
from bitty.consumers.poloniex import PoloniexConsumer

json_dumps = ujson.dumps  # pylint: disable=no-member
json_loads = ujson.loads  # pylint: disable=no-member

logger = logging.getLogger(__name__)


HEARTBEAT_MSG = {
    'type': 'heartbeat',
    'on': True
}


PRODUCT_PAIRS = (
    ['BTC-USD'],
    ['ETH-USD'],
    ['ETH-BTC'],
    ['LTC-USD'],
    ['LTC-BTC']
)


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


PUBLISH_MESSAGE_ATTRS = ['order_id', 'maker_order_id', 'taker_order_id',
                         'user_id']


class ThreadKeeper:
    ws = None
    url = ''
    terminated = False
    last_heartbeat = None
    keepalive = None
    trade_keepalive = None
    consumer = None
    product_ids = None

    def __init__(self, url, product_ids, loop=None):
        self.url = url
        self.product_ids = product_ids
        self.loop = loop

        self.last_heartbeat = {}

    def spawn_consumer(self):
        self.consumer = self.loop.create_task(self.consume())
        self.consumer.add_done_callback(self.on_consume_end)

    async def kill(self):
        logger.warning('requested consumer kill. Terminating app')
        if self.terminated:
            return

        self.terminated = True
        if self.ws:
            await self.ws.close()

    async def reconnect(self, *, kill_keepalive=True, kill_consumer=True):
        if kill_keepalive and self.keepalive:
            self.keepalive.cancel()
        if kill_keepalive and self.trade_keepalive:
            self.trade_keepalive.cancel()

        if kill_consumer and self.consumer:
            self.consumer.remove_done_callback(self.on_consume_end)
            self.consumer.cancel()

        logger.info('reconnecting.... closing ws')
        await self.ws.close()
        self.spawn_consumer()

    def on_consume_end(self, task):
        if task is not self.consumer:
            logger.warning('got wrong task on consume end')
        if not task.cancelled():
            exc = task.exception()
            if exc:
                logger.error('consume had an error %s', exc,
                             exc_info=sys.exc_info())
            if not self.terminated:
                self.loop.create_task(self.reconnect())

    async def consume(self):
        if self.terminated:
            logger.trace('Termination requested. not consuming')
            return

        try:
            session = aiohttp.ClientSession()
            async with session.ws_connect(self.url) as ws:
                self.ws = ws
                await self.subscribe()
                await self.spawn_keepalive()
                try:
                    logger.info('consuming %s', self.product_ids)
                    error_data = await self._consume()
                finally:
                    await self.ws.close()

                logger.warning('consume ended; ws closed')

                if not self.terminated and error_data is not None:
                    logger.error(error_data)
        except CancelledError:
            raise
        except:  # pylint:disable=bare-except
            logger.exception('Consume failed')
        finally:
            session.close()

    async def _consume(self):
        error_data = None
        async for msg in self.ws:
            if self.terminated:
                if error_data is None:
                    error_data = 'bank app terminated'
                break

            error_data = msg.data
            if msg.type == aiohttp.WSMsgType.TEXT:
                if msg.data == 'close cmd':
                    logger.warning('close cmd')
                    break

                error_data = None
                try:
                    self.on_message(msg)
                except:  # pylint: disable=bare-except
                    logger.exception('ws on_message had an error')

            elif msg.type == aiohttp.WSMsgType.CLOSED:
                logger.warning('closed')
                break
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.warning('error')
                break
        return error_data

    def on_message(self, m):
        try:
            if isinstance(m, str):
                msg_data = json_loads(m)
            else:
                msg_data = json_loads(m.data)

            msg_type = msg_data.get('type')
            if msg_type not in ('done', 'received', 'open', 'heartbeat'):
                logger.debug(msg_data)
            if msg_type == 'error':
                logger.warning('dropping message %s', msg_data)
                return
            msg = models.from_json(msg_data)
            if isinstance(msg, models.HeartbeatMessage):
                self.on_heartbeat(msg)
            elif isinstance(msg, models.MatchMessage):
                msg_data['type'] = 'trade'
                trade_msg = models.from_json(msg_data)
                self.on_trade(trade_msg)
        except:
            logger.exception('error in on_message')
            raise

    async def subscribe(self):
        subscribe_msg = copy.deepcopy(SUBSCRIBE_MSG)
        subscribe_msg['product_ids'] = self.product_ids
        # For some reason, if these are sent at the same time
        # one doesn't work
        await self.send_message(subscribe_msg)

    async def send_message(self, msg):
        logger.info('sending message %s', msg)
        await self.ws.send_json(msg, dumps=json_dumps)

    async def spawn_keepalive(self):
        logger.info('spawning keepalive %s', self.product_ids)
        await self.send_message(HEARTBEAT_MSG)
        self.keepalive = self.loop.create_task(self._keepalive('heartbeat'))
        self.trade_keepalive = self.loop.create_task(self._keepalive('trade'))

    def on_trade(self, trade):
        self.last_heartbeat['trade'] = time.time()
        self.last_heartbeat['trade_trade'] = trade.trade_id

        icon = '📈' if trade.side == 'sell' else '📉'
        logger.info('%s :: %s :: %s %s%s   %s %s', self.product_ids,
                    trade.product_id, trade.size,
                    trade.price, icon, trade.trade_id, trade.time)

    def on_heartbeat(self, msg):
        self.last_heartbeat['heartbeat'] = time.time()
        self.last_heartbeat['heartbeat_trade'] = msg.last_trade_id

        logger.info('%s :: heartbeat :: %s %s', self.product_ids,
                    msg.last_trade_id, msg.time)

    async def _keepalive(self, ka_type):
        self.last_heartbeat[ka_type] = time.time()

        max_time_allowed = 2 if ka_type == 'heartbeat' else 70  # Seconds

        while True:
            await asyncio.sleep(max_time_allowed, loop=self.loop)
            if self.terminated:
                logger.trace('termination requested. ending keep alive')
                break
            cur_time = time.time()
            time_elapsed = cur_time - self.last_heartbeat[ka_type]

            # if this is a trade check and
            # the last trade id reported by the 'heartbeat' ws msg
            # is the same as the last trade we saw on the ws
            # dont mark the trade heartbeat missed if we haven't had
            # one in over 70 secs
            last_trade = self.last_heartbeat['trade_trade']
            last_heartbeat_trade = self.last_heartbeat['heartbeat_trade']
            has_last_trade = last_trade == last_heartbeat_trade
            missed_heartbeat = time_elapsed > max_time_allowed
            if ka_type == 'trade' and has_last_trade and missed_heartbeat:
                missed_heartbeat = False

            if missed_heartbeat:
                logger.warning('%s :: %s missed heartbeat after %s secs (max is %s)',
                               self.product_ids, ka_type, time_elapsed, max_time_allowed)
                self.loop.create_task(self.reconnect())
                break
            else:
                logger.debug('%s :: %s satisfied heartbeat after %s secs',
                             self.product_ids, ka_type, time_elapsed)


def consume(loop=None):
    if loop is None:
        loop = asyncio.get_event_loop()

    """
    threads = []
    for product_ids in PRODUCT_PAIRS:
        logger.info('creating thread for pair: %s', product_ids)
        keeper = ThreadKeeper(url, product_ids, loop=loop)
        keeper.spawn_consumer()
        threads.append(keeper)
    """
    threads = []
    """
    for product_ids in PRODUCT_PAIRS:
        logger.info('creating thread for pair: %s', product_ids)
        keeper = GdaxConsumer(product_ids, loop=loop)
        keeper.spawn_consumer()
        threads.append(keeper)

    """
    keeper = PoloniexConsumer(['USDT_BTC'], loop=loop)
    keeper.spawn_consumer()
    threads.append(keeper)

    return threads


if __name__ == '__main__':
    loggers.setup()
    loop = asyncio.get_event_loop()
    threads = consume(loop=loop)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        logger.info('exiting')
        cos = []
        for thread in threads:
            cos.append(thread.kill())

        loop.run_until_complete(asyncio.gather(*cos, loop=loop))
        loop.stop()
