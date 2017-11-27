import logging
import inspect
import random

from autobahn.wamp import message
from autobahn.wamp.role import DEFAULT_CLIENT_ROLES
from autobahn.wamp.serializer import JsonSerializer


logger = logging.getLogger(__name__)


class WAMPClient:
    def __init__(self,
                 url,
                 roles=DEFAULT_CLIENT_ROLES,
                 realm='realm1',
                 protocols=('wamp.2.json',),
                 serializer=JsonSerializer()):


        self.url = url
        self.protocols = protocols
        self.realm = realm
        self.roles = roles
        self.serializer = serializer
        self.ws = None
        self.need_stop = False
        self.connected = False
        self.handlers = {
            message.Welcome.MESSAGE_TYPE: self._on_welcome,
            message.Subscribed.MESSAGE_TYPE: self._on_subscribed,
            message.Event.MESSAGE_TYPE: self._on_event,
        }

        self.queue = {}
        self.subscriptions = {}

        self.on_welcome_callback = None

    def get_handler(self, message_type):
        handler = self.handlers.get(message_type)
        if handler is None:
            handler = self._on_other
        return handler

    def on_welcome(self, handler):
        self.on_welcome_callback = handler

    async def _on_welcome(self, msg):
        self.connected = True

        if callable(self.on_welcome_callback):
            if inspect.iscoroutinefunction(self.on_welcome_callback):
                await self.on_welcome_callback()
            else:
                self.on_welcome_callback()

        for request_id, subscription in self.queue.items():
            topic = subscription['topic']
            subscribe = message.Subscribe(request=request_id, topic=topic)
            self.send(subscribe)

    async def _on_event(self, event):
        subscription_id = event.subscription
        subscription = self.subscriptions[subscription_id]

        handler = subscription['handler']
        if inspect.iscoroutinefunction(handler):
            await handler(event.args)
        else:
            handler(event.args)

    async def _on_subscribed(self, msg):
        request_id = msg.request
        subscription_id = msg.subscription

        subscription = self.queue.pop(request_id)
        subscription['request_id'] = request_id
        self.subscriptions[subscription_id] = subscription

    async def _on_other(self, msg):
        logger.warning('weird msg %s', msg)

    def send(self, msg):
        payload, _ = self.serializer.serialize(msg)
        self.ws.send_str(payload.decode())

    def recv(self, s):
        messages = self.serializer.unserialize(s.encode())
        return messages[0]

    async def start(self, session):
        async with session.ws_connect(url=self.url,
                                      protocols=self.protocols) as ws:
            self.ws = ws

            if self.need_stop:
                await self.stop()
                return

            hello = message.Hello(self.realm, self.roles)
            self.send(hello)

            async for ws_msg in ws:
                wamp_msg = self.recv(ws_msg.data)
                wamp_handler = self.get_handler(wamp_msg.MESSAGE_TYPE)
                await wamp_handler(wamp_msg)

    async def stop(self):
        if self.ws:
            await self.ws.close()
        else:
            self.need_stop = True

    def subscribe(self, handler, topic):
        request_id = random.randint(10 ** 14, 10 ** 15 - 1)
        subscription = {
            'topic': topic,
            'handler': handler
        }

        if self.connected:
            self.queue[request_id] = subscription
            self.send(message.Subscribe(request=request_id, topic=topic))
        else:
            self.queue[request_id] = subscription
