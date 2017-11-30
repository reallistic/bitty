import time
import asyncio
import aiohttp
import copy
import logging
import ujson
import sys
import inspect

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
    on_trade_callback = None
    exhange_name = ''
    ws = None

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
        logger.info('%s : sending message %s', self.exchange_name, msg)
        await self.ws.send_json(msg, dumps=json_dumps)

    def spawn_consumer(self):
        self.consumer = self.loop.create_task(self.consume())
        self.consumer.add_done_callback(self.on_consume_end)

    async def spawn_keepalive(self):
        logger.info('%s : spawning keepalive %s', self.exchange_name,
                    self.product_ids)
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
                logger.debug('%s : termination requested. ending keep alive',
                             self.exchange_name)
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
                logger.warning('%s : %s :: %s missed heartbeat after %s '
                               'secs (max is %s)',
                               self.exchange_name, self.product_ids, ka_type,
                               time_elapsed, max_time_allowed)
                self.loop.create_task(self.reconnect())
                break
            else:
                logger.debug('%s : %s :: %s satisfied heartbeat after %s secs',
                             self.exchange_name, self.product_ids, ka_type,
                             time_elapsed)

    async def kill(self):
        logger.warning('%s : requested consumer kill. Terminating app',
                       self.exchange_name)
        if self.terminated:
            return

        self.terminated = True
        if self.ws:
            await self.ws.close()

        if self.keepalive:
            self.keepalive.cancel()
        if self.trade_keepalive:
            self.trade_keepalive.cancel()

        if self.consumer:
            self.consumer.remove_done_callback(self.on_consume_end)
            self.consumer.cancel()

    async def reconnect(self, *, kill_keepalive=True, kill_consumer=True):
        if kill_keepalive and self.keepalive:
            self.keepalive.cancel()
        if kill_keepalive and self.trade_keepalive:
            self.trade_keepalive.cancel()

        if kill_consumer and self.consumer:
            self.consumer.remove_done_callback(self.on_consume_end)
            self.consumer.cancel()

        if self.ws:
            await self.ws.close()
            ws_msg = 'closing ws'
        else:
            ws_msg = 'ws never opened'
        logger.info('%s : reconnecting.... %s', self.exchange_name, ws_msg)
        self.spawn_consumer()

    def on_consume_end(self, task):
        if task is not self.consumer:
            logger.warning('%s : got wrong task on consume end',
                           self.exchange_name)
        if not task.cancelled():
            exc = task.exception()
            if exc:
                logger.error('%s : consume had an error %s', exc,
                             self.exchange_name, exc_info=sys.exc_info())
            if not self.terminated:
                self.loop.create_task(self.reconnect())

    async def consume(self):
        if self.terminated:
            logger.info('%s : Termination requested. not consuming',
                         self.exchange_name)
            return

        session = aiohttp.ClientSession()
        try:
            logger.info('%s : ws_connect %s', self.exchange_name, self.url)
            async with session.ws_connect(self.url) as ws:
                self.ws = ws

                logger.info('%s : ws connection active, sending subscribe',
                            self.exchange_name)

                await self.subscribe()
                await self.spawn_keepalive()
                logger.info('%s : consuming %s', self.product_ids,
                            self.exchange_name)
                try:
                    error_data = await self._consume()
                finally:
                    await self.ws.close()

                logger.warning('%s : consume ended; ws closed',
                               self.exchange_name)

                if not self.terminated and error_data is not None:
                    logger.error('%s : %s', self.exchange_name, error_data)
        except CancelledError:
            raise
        except:  # pylint:disable=bare-except
            logger.exception('%s : Consume failed', self.exchange_name)
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
                    logger.warning('%s : close cmd', self.exchange_name)
                    break

                error_data = None
                try:
                    self.on_message(msg.data)
                except:  # pylint: disable=bare-except
                    logger.exception('%s ws on_message had an error',
                                     self.exchange_name)

            elif msg.type == aiohttp.WSMsgType.CLOSED:
                logger.warning('%s : closed', self.exchange_name)
                break
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.warning('%s : error', self.exchange_name)
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
                logger.warning('%s : dropping message %s',
                               self.exchange_name, msg_data)
                return
            msg_data.setdefault('exchange_name', self.exchange_name)

            msg = models.from_json(msg_data)

            if isinstance(msg, models.HeartbeatMessage):
                self.collect_heartbeat('heartbeat', msg.last_trade_id)
                self.on_heartbeat(msg)
            elif isinstance(msg, models.MatchMessage):
                msg_data['type'] = 'trade'
                trade_msg = models.from_json(msg_data)
                self.collect_heartbeat('trade', trade_msg.trade_id)
                self._on_trade(trade_msg)
            elif isinstance(msg, models.TradeMessage):
                self.collect_heartbeat('trade', msg.trade_id)
                self._on_trade(msg)

    def on_trade(self, callback):
        self.on_trade_callback = callback

    def _on_trade(self, trade):
        if callable(self.on_trade_callback):
            if inspect.iscoroutinefunction(self.on_trade_callback):
                self.loop.create_task(self.on_trade_callback(trade))
            else:
                self.on_trade_callback(trade)

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
