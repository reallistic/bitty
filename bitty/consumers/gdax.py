import copy
import logging
import ujson

from bitty.consumers.base import BaseConsumer


json_loads = ujson.loads  # pylint: disable=no-member


logger = logging.getLogger(__name__)


HEARTBEAT_MSG = {
    'type': 'heartbeat',
    'on': True
}


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


class GdaxConsumer(BaseConsumer):
    enable_heartbeat = True
    url = 'wss://ws-feed.gdax.com'
    exchange_name = 'gdax'

    def _on_trade(self, trade):
        super()._on_trade(trade)
        icon = '📈' if trade.side == 'sell' else '📉'
        logger.info('%s :: %s :: %s %s%s   %s %s', self.product_ids,
                    trade.product_id, trade.size,
                    trade.price, icon, trade.trade_id, trade.time)

    def on_heartbeat(self, message):
        logger.info('%s :: heartbeat :: %s %s', self.product_ids,
                    message.last_trade_id, message.time)

    def process_message(self, message):
        try:
            msg_data = json_loads(message)
        except:
            logger.error('message error %s', message)
            raise
        return msg_data

    def make_subscribe_payload(self):
        subscribe_msg = copy.deepcopy(SUBSCRIBE_MSG)
        subscribe_msg['product_ids'] = self.product_ids
        return subscribe_msg

    def make_heartbeat_payload(self):
        return HEARTBEAT_MSG
