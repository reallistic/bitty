import logging

logger = logging.getLogger(__name__)

MessageMap = {}

# pylint:disable=too-few-public-methods

class Message:
    price = 0
    product_id = ''
    side = ''
    time = ''
    sequence = 0
    type = ''
    product = None

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            if hasattr(self, key):
                setattr(self, key, value)


class HeartbeatMessage(Message):
    last_trade_id = 0


class OrderBookMessage(Message):
    order_id = ''


class ReceivedMessage(OrderBookMessage):
    """Market order have a 'funds' field others have 'size'"""
    order_type = ''
    funds = 0
    size = 0


class OpenMessage(OrderBookMessage):
    remaining_size = 0


class DoneMessage(OrderBookMessage):
    """Market order have a 'funds' field others have 'size'"""
    reason = ''
    remaining_size = 0


class MatchMessage(Message):
    """
    A trade occurred between these two.

    Taker order is executed immediately after being received.
    Maker order was already on the book.
    The side field indicates maker order side.
    If the side is sell match is considered an up-tick.
    A buy side match is a down-tick.

    user/profile fields require auth and you must be taker
    """
    size = 0
    trade_id = 0
    maker_order_id = ''
    taker_order_id = ''

    taker_user_id = ''
    user_id = ''
    taker_profile_id = ''
    profile_id = ''


class ChangeMessage(OrderBookMessage):
    """
    Market order have a 'funds' field others have 'size'

    When price is null this is for a market order
    """
    new_size = 0
    old_size = 0

    new_funds = 0
    old_funds = 0


class TradeMessage(Message):
    size = 0
    trade_id = 0

    def _get_dict(self):
        rv = {
            'price': self.price,
            'product_id': self.product_id,
            'side': self.side,
            'time': self.time,
            'sequence': self.sequence,
            'type': self.type,
            'size': self.size,
            'trade_id': self.trade_id
        }

        return rv


class FillMessage(TradeMessage):
    order_id = ''
    settled = False
    liquidity = ''
    fee = 0


class OrderResponse(Message):
    size = 0
    order_type = ''
    fill_fees = 0
    filled_size = 0
    order_id = ''
    done_at = 0
    done_reason = ''
    reject_reason = ''
    executed_value = 0
    settled = False
    status = ''
    updated_time = None

    def before_init(self, **kwargs):
        kwargs = super(OrderResponse, self).before_init(**kwargs)
        kwargs['order_id'] = kwargs.pop('id')
        kwargs['time'] = kwargs.pop('created_at')
        return kwargs

    def is_filled(self):
        return self.settled and self.done_reason == 'filled'

    def __str__(self):
        args = (self.side, self.size, self.product_id, self.price,
                self.filled_size, self.settled, self.status)
        return (
            '<Order %s %s %s at %s filled_size=%s settled=%s status=%s>' %
            args
        )


MessageMap['change'] = ChangeMessage
MessageMap['done'] = DoneMessage
MessageMap['match'] = MatchMessage
MessageMap['open'] = OpenMessage
MessageMap['received'] = ReceivedMessage
MessageMap['trade'] = TradeMessage
MessageMap['fill'] = FillMessage
MessageMap['order_response'] = OrderResponse
MessageMap['heartbeat'] = HeartbeatMessage


def from_json(msg):
    msg_type = msg.get('type')
    order_id = msg.get('order_id')
    if msg_type not in MessageMap:
        return None
    cls = MessageMap.get(msg_type, OrderBookMessage if order_id else Message)

    return cls(**msg)
