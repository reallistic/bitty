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
    terminated = False
    keepalive = None
    trade_keepalive = None
    consumer = None
    ws = None

    ###
    ## Per consumer overrides
    ###
    url = None
    exchange_name = ''
    enable_heartbeat = False

    # Keep alive timeouts in Seconds
    heartbeat_timeout = 2
    trade_timeout = 70

    def __init__(self, product_ids, url=None, loop=None):
        self.product_ids = product_ids
        self.loop = loop

        self.last_heartbeat = {}

        self.on_trade_callbacks = []
        self.on_heartbeat_callbacks = []

        if url is not None:
            self.url = url
        if self.url is None:
            raise RuntimeError('need url for consumer %s' % self.__class__.__name__)

    @property
    def consumer_id(self):
        if len(self.product_ids) == 1:
            product_ids = self.product_ids[0]
        else:
            product_ids = self.product_ids
        return '%s | %s' % (self.exchange_name, product_ids)

    async def subscribe(self):
        subscribe_msg = self.make_subscribe_payload()
        await self.send_message(subscribe_msg)

    async def send_message(self, msg):
        logger.info('%s : sending message %s', self.consumer_id, msg)
        await self.ws.send_json(msg, dumps=json_dumps)

    def spawn_consumer(self):
        self.consumer = self.loop.create_task(self.consume())
        self.consumer.add_done_callback(self.on_consume_end)

    async def spawn_keepalive(self):
        logger.info('%s : spawning keepalive', self.consumer_id)
        self.trade_keepalive = self.loop.create_task(self._keepalive('trade'))

        if self.enable_heartbeat:
            heartbeat_msg = self.make_heartbeat_payload()
            if heartbeat_msg is not None:
                await self.send_message(heartbeat_msg)
            self.keepalive = self.loop.create_task(self._keepalive('heartbeat'))

    def collect_heartbeat(self, hb_type, trade_id):
        self.last_heartbeat[hb_type] = time.time()
        self.last_heartbeat[hb_type + '_trade'] = trade_id

    async def _keepalive(self, ka_type):
        self.last_heartbeat[ka_type] = time.time()

        if ka_type == 'heartbeat':
            other_ka_type = 'trade'
            max_time_allowed = self.heartbeat_timeout
        else:
            other_ka_type = 'heartbeat'
            max_time_allowed = self.trade_timeout

        while True:
            await asyncio.sleep(max_time_allowed, loop=self.loop)
            if self.terminated:
                logger.debug('%s : termination requested. ending keep alive',
                             self.consumer_id)
                break
            cur_time = time.time()
            time_elapsed = cur_time - self.last_heartbeat[ka_type]
            other_time_elapsed = cur_time - self.last_heartbeat[other_ka_type]

            missed_heartbeat = time_elapsed > max_time_allowed
            made_other_hb = other_time_elapsed < max_time_allowed
            # if this is a trade check and
            # the last trade id reported by the 'heartbeat' ws msg
            # is the same as the last trade we saw on the ws
            # dont mark the trade heartbeat missed if we haven't had
            # one within the timeout period.
            if self.enable_heartbeat:
                last_trade = self.last_heartbeat.get('trade_trade')
                last_heartbeat_trade = self.last_heartbeat.get('heartbeat_trade')
                has_last_trade = last_trade == last_heartbeat_trade
                if ka_type == 'trade' and has_last_trade and missed_heartbeat:
                    missed_heartbeat = False
                elif ka_type == 'heartbeat' and made_other_hb:
                    missed_heartbeat = False

            if missed_heartbeat:
                logger.warning('%s :: %s missed heartbeat after %s '
                               'secs and %s secs (max is %s)',
                               self.consumer_id, ka_type,
                               round(time_elapsed, 2),
                               round(other_time_elapsed, 2),
                               max_time_allowed)
                self.loop.create_task(self.reconnect())
                break
            else:
                logger.debug('%s :: %s satisfied heartbeat after %s secs',
                             self.consumer_id, ka_type, time_elapsed)

    async def kill(self):
        logger.warning('%s : requested consumer kill. Terminating app',
                       self.consumer_id)
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
        logger.info('%s : reconnecting.... %s', self.consumer_id, ws_msg)
        self.spawn_consumer()

    def on_consume_end(self, task):
        if task is not self.consumer:
            logger.warning('%s : got wrong task on consume end',
                           self.consumer_id)
        if not task.cancelled():
            exc = task.exception()
            if exc:
                logger.error('%s : consume had an error %s', exc,
                             self.consumer_id, exc_info=sys.exc_info())
            if not self.terminated:
                self.loop.create_task(self.reconnect())

    async def consume(self):
        if self.terminated:
            logger.info('%s : Termination requested. not consuming',
                         self.consumer_id)
            return

        session = aiohttp.ClientSession()
        try:
            logger.info('%s : ws_connect %s', self.consumer_id, self.url)
            async with session.ws_connect(self.url) as ws:
                self.ws = ws

                logger.info('%s : ws connection active, sending subscribe',
                            self.consumer_id)

                await self.subscribe()
                await self.spawn_keepalive()
                logger.info('%s : consuming', self.consumer_id)
                try:
                    error_data = await self._consume()
                finally:
                    await self.ws.close()

                logger.warning('%s : consume ended; ws closed',
                               self.consumer_id)

                if not self.terminated and error_data is not None:
                    logger.error('%s : %s', self.consumer_id, error_data)
        except CancelledError:
            raise
        except:  # pylint:disable=bare-except
            logger.exception('%s : Consume failed', self.consumer_id)
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
                    logger.warning('%s : close cmd', self.consumer_id)
                    break

                error_data = None
                try:
                    self.on_message(msg.data)
                except:  # pylint: disable=bare-except
                    logger.exception('%s ws on_message had an error',
                                     self.consumer_id)

            elif msg.type == aiohttp.WSMsgType.CLOSED:
                logger.warning('%s : closed', self.consumer_id)
                break
            elif msg.type == aiohttp.WSMsgType.ERROR:
                logger.warning('%s : error', self.consumer_id)
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
                               self.consumer_id, msg_data)
                return
            msg_data.setdefault('exchange_name', self.exchange_name)

            msg = models.from_json(msg_data)

            if isinstance(msg, models.HeartbeatMessage):
                self.collect_heartbeat('heartbeat', msg.last_trade_id)
                self._on_heartbeat(msg)
            elif isinstance(msg, models.MatchMessage):
                msg_data['type'] = 'trade'
                trade_msg = models.from_json(msg_data)
                self.collect_heartbeat('trade', trade_msg.trade_id)
                self._on_trade(trade_msg)
            elif isinstance(msg, models.TradeMessage):
                self.collect_heartbeat('trade', msg.trade_id)
                self._on_trade(msg)

    def on_trade(self, callback):
        self.on_trade_callbacks.append(callback)

    def _on_trade(self, trade):
        for callback in self.on_trade_callbacks:
            if callable(callback):
                if inspect.iscoroutinefunction(callback):
                    self.loop.create_task(callback(trade))
                else:
                    callback(trade)

    def on_heartbeat(self, callback):
        self.on_heartbeat_callbacks.append(callback)

    def _on_heartbeat(self, msg):
        for callback in self.on_heartbeat_callbacks:
            if callable(callback):
                if inspect.iscoroutinefunction(callback):
                    self.loop.create_task(callback(msg))
                else:
                    callback(msg)

    def is_connected(self):
        # Haven't consumed yet
        if not self.ws:
            return False
        if self.ws.closed:
            return False
        return True

    def get_status(self):
        return dict(
            heartbeats=self.last_heartbeat,
            enable_ws_heartbeat=self.enable_heartbeat,
            connected=self.is_connected()
        )


    def process_message(self, message):
        """
        This should take a json string and return a dict
        formatted for the models.
        If the message should be dropped this should return None
        """
        raise NotImplementedError('must implement it')

    def make_subscribe_payload(self):
        """
        This should return a json dict with the format
        of the subscribe message to be sent directly
        to the exchange
        """
        raise NotImplementedError('must implement it')

    def make_heartbeat_payload(self):
        """
        This should return a json dict with the format
        of the heartbeat message to be sent directly
        to the exchange.
        Return None if a message does not need to be sent
        in order to receive heartbeat messages from the ws.
        NOTE: This is only called if enable_heartbeat is True.
        """
        raise NotImplementedError('must implement it')
