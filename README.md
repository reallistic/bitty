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
    enable_heartbeat = False # True if this exchange supports heartbeat messages

    def on_trade(self, trade):
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
        pass
```


# Enabling more pairs/exchanges

This is currently done manually in bitty/collect.py


# NOTES

- GDAX can subscribe to multiple product pairs but this sometimes causes things to not work. Also, heartbeat validation would need to be done on a per-pair basis where as it is currently generic to only one.
