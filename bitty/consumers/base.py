import time
import asyncio
import aiohttp
import copy
import logging
import ujson
import sys

from concurrent.futures import CancelledError
from bitty import models


json_dumps = ujson.dumps  # pylint: disable=no-member

logger = logging.getLogger(__name__)


class BaseConsumer:
    enable_heartbeat = False
    url = None
    terminated = False
    keepalive = None
    trade_keepalive = None
    consumer = None

    def __init__(self, product_ids, url=None, loop=None):
        self.product_ids = product_ids
        self.loop = loop

        self.last_heartbeat = {}

        if url is not None:
            self.url = url
        if self.url is None:
            raise RuntimeError('need url for consumer %s' % self.__class__.__name__)

    async def subscribe(self):
        subscribe_msg = self.make_subscribe_payload()
        await self.send_message(subscribe_msg)

    async def send_message(self, msg):
        logger.info('sending message %s', msg)
        await self.ws.send_json(msg, dumps=json_dumps)

    def spawn_consumer(self):
        self.consumer = self.loop.create_task(self.consume())
        self.consumer.add_done_callback(self.on_consume_end)

    async def spawn_keepalive(self):
        logger.info('spawning keepalive %s', self.product_ids)
        self.trade_keepalive = self.loop.create_task(self._keepalive('trade'))

        if self.enable_heartbeat:
            heartbeat_msg = self.make_heartbeat_payload()
            await self.send_message(heartbeat_msg)
            self.keepalive = self.loop.create_task(self._keepalive('heartbeat'))

    def collect_heartbeat(self, hb_type, trade_id):
        self.last_heartbeat[hb_type] = time.time()
        self.last_heartbeat[hb_type + '_trade'] = trade_id

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

            missed_heartbeat = time_elapsed > max_time_allowed
            # if this is a trade check and
            # the last trade id reported by the 'heartbeat' ws msg
            # is the same as the last trade we saw on the ws
            # dont mark the trade heartbeat missed if we haven't had
            # one in over 70 secs
            if self.enable_heartbeat:
                last_trade = self.last_heartbeat.get('trade_trade')
                last_heartbeat_trade = self.last_heartbeat.get('heartbeat_trade')
                has_last_trade = last_trade == last_heartbeat_trade
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
                    self.on_message(msg.data)
                except:  # pylint: disable=bare-except
                    logger.exception('ws on_message had an error')

            elif msg.type == aiohttp.WSMsgType.CLOSED:
                logger.warning('closed')
                break
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.warning('error')
                break
        return error_data

    def on_message(self, m, **kwargs):
        messages = self.process_message(m, **kwargs)
        if messages is None:
            return

        if not isinstance(messages, list):
            messages = [messages]

        for msg_data in messages:
            msg_type = msg_data.get('type')
            if msg_type == 'error':
                logger.warning('dropping message %s', msg_data)
                return

            msg = models.from_json(msg_data)

            if isinstance(msg, models.HeartbeatMessage):
                self.collect_heartbeat('heartbeat', msg.last_trade_id)
                self.on_heartbeat(msg)
            elif isinstance(msg, models.MatchMessage):
                msg_data['type'] = 'trade'
                trade_msg = models.from_json(msg_data)
                self.collect_heartbeat('trade', trade_msg.trade_id)
                self.on_trade(trade_msg)
            elif isinstance(msg, models.TradeMessage):
                self.collect_heartbeat('trade', msg.trade_id)
                self.on_trade(msg)

    def on_trade(self, trade):
        raise NotImplementedError('must implement it')

    def on_heartbeat(self, msg):
        raise NotImplementedError('must implement it')

    def process_message(self, message):
        """
        This should take a json string and return a dict
        formatted for the models.
        If the message should be dropped this should return None
        """
        raise NotImplementedError('must implement it')

    def make_subscribe_payload(self):
        # subscribe_msg = copy.deepcopy(SUBSCRIBE_MSG)
        # subscribe_msg['product_ids'] = self.product_ids
        raise NotImplementedError('must implement it')

    def make_heartbeat_payload(self):
        # return HEARTBEAT_MSG
        raise NotImplementedError('must implement it')
