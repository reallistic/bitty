Bitty
=========


# Installation

```
pip install -e .
```


# Running

```
LOG_LEVEL=info python -m bitty.collect
# or
LOG_LEVEL=info bitty
```


# Adding more exchanges

Each exchange should implement the `BaseConsumer` class in bitty/consumers/base.py.

for example:

```
class MyFancyExchangeConsumer(BaseConsumer):

    url = 'websocket url'
    enable_heartbeat = False # Set to true if this exchange supports heartbeat messages
    exchange_name = 'MyFancyExchange' # Used for logging and callbacks
    heartbeat_timeout = 2 # how many seconds to allow in between heartbeats
    trade_timeout = 70 # How many seconds to allow in between trades

    def _on_trade(self, trade):
        # Call super so callbacks get called
        super()._on_trade(trade)
        # We got a trade, do what you wish with it
        pass

    def on_heartbeat(self, message):
        # We got a heartbeat, do what you wish with it
        pass

    def process_message(self, message):
        # Take a websocket item and convert it to a dict formatted to be readable by
        # a model in bitty/models.py.
        # Or return `None` to ignore this message
        pass

    def make_subscribe_payload(self):
        # This will be sent directly to the websocket to subscribe for updates
        pass

    def make_heartbeat_payload(self):
        # This will be sent directly to the websocket to subscribe for heartbeats
        Return None if a message does not need to be sent
        in order to receive heartbeat messages from the ws.
        NOTE: This is only called if enable_heartbeat is True.
        pass
```

Callbacks can be added as well:

```
def foo(trade):
    pass

consumer = MyFancyExchangeConsumer()
consumer.on_trade(foo)
```



# Enabling more pairs/exchanges

This is currently done manually in bitty/collect.py


# NOTES

- GDAX can subscribe to multiple product pairs but this sometimes causes things to not work. Also, heartbeat validation would need to be done on a per-pair basis where as it is currently generic to only one.
