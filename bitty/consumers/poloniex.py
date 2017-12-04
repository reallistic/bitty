import copy
import logging
import ujson
import aiohttp
import sys

from functools import partial
from concurrent.futures import CancelledError
from bitty.consumers.base import BaseConsumer

from bitty import utils


json_loads = ujson.loads  # pylint: disable=no-member


logger = logging.getLogger(__name__)


SUBSCRIBE_MSG = {
    'channel': 'USDT_BTC',
    'command': 'subscribe'
}


class PoloniexConsumer(BaseConsumer):
    enable_heartbeat = True
    url = 'wss://api2.poloniex.com'
    exchange_name = 'poloniex'
    market_channel = None
    heartbeat_timeout = 15
    trade_timeout = 70

    def process_message(self, message, product_id=None):
        """
        Processes messages and returns a list of trade and/or
        heartbeat models.
        Poloniex sends back a integer identifier for each message
        type. They aren't documented on the api site so I pulled
        them from the javascript implementation of the polo UI.

        1000: messages about the logged in user's balance and real time orders
        1001: trolbox events
        1002: ticker events
        1003: server stats like current time, users online, etc
        1010: heartbeat
        2000: alerts (?) including "cancelOrder" and "cancelTriggerOrder"
        1-999: market specific data such as order book changes and trades
        """

        msg = json_loads(message)
        rv = []

        msg_iden = msg[0] if isinstance(msg, list) else None

        if not isinstance(msg_iden, int):
            logger.info('%s : weird msg %s', self.consumer_id, msg)
            return None

        # The first message or so should be an information message
        # that tells us what the `msg_iden` will be for trade updates.
        # It also gives us the full current order book.
        # NOTE: The below logic is basically copied from:
        #   https://poloniex.com/js/plx_exchage.js?v=081217
        if msg_iden > 0 and msg_iden < 1000:
            if msg[2][0][0] == 'i':
                market_info = msg[2][0][1]
                if market_info.get('currencyPair') not in self.product_ids:
                    logger.warning('%s got market info for wrong pair %s',
                                   self.consumer_id, market_info)
                    return None
                order_book = market_info.pop('orderBook')
                self.market_channel = msg_iden
                return None

        if msg_iden == 1010:
            rv.append({
                'type': 'heartbeat',
                'product_id': self.product_ids[0],
                'time': utils.get_iso8601(),
                'last_trade_id': None
            })
            return rv

        if msg_iden == self.market_channel: # Order book or trade
            seq = msg[1]
            messages = msg[2]

        for msg in messages:
            msg_type = msg[0]
            if msg_type == 't': # Trade
                rv.append({
                    'type': 'trade',
                    'price': msg[3],
                    'product_id': self.product_ids[0],
                    'side': 'buy' if msg[2] else 'sell',
                    'time': utils.iso8601_from_secs(msg[5]),
                    'sequence': seq,
                    'size': msg[4],
                    'trade_id': msg[1]
                })
        if rv:
            return rv
        return None

    def make_subscribe_payload(self):
        subscribe_msg = copy.deepcopy(SUBSCRIBE_MSG)
        subscribe_msg['channel'] = self.product_ids[0]
        return subscribe_msg

    def make_heartbeat_payload(self):
        return None
