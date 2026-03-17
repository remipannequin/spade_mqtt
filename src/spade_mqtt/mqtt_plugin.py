"""Spade plugin to communicate over MQTT. Heavily inspired by aiomqtt."""

from typing import Any
import asyncio


import paho.mqtt.client as mqtt
from loguru import logger

from paho.mqtt.properties import Properties, PacketTypes
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.client import topic_matches_sub


class MqttQueues:
    """Object where the state of the mqtt client is stored."""

    def __init__(self, loop: asyncio.AbstractEventLoop):
        self._msg_queues: dict[str, asyncio.Queue] = {}
        # Publish events
        self._pending_publish: dict[int, asyncio.Event] = {}
        # Connected event
        self._pending_connect: asyncio.Event | None = None
        # Disconnected event
        self._pending_disconnect: asyncio.Event | None = None
        # Subscribed events
        self._pending_subscribe: dict[int, asyncio.Event] = {}
        # Unsubscribed events
        self._pending_unsubscribe: dict[int, asyncio.Event] = {}
        self._loop = loop

    def topic_list(self):
        """Return the list of topic currently subscribed."""
        return self._msg_queues.keys()

    def connect_start(self) -> asyncio.Task:
        """Create an event to be set by the connect callback, and return a
        task that wait for it."""
        self._pending_connect = asyncio.Event()
        return asyncio.create_task(self._pending_connect.wait(), name="connected")

    def connect_done(self):
        """Called when the connection is done"""
        if self._pending_connect is not None:
            self._loop.call_soon_threadsafe(self._pending_connect.set)

    def has_pending_connect(self) -> bool:
        """Return true if a connection is pending"""
        return self._pending_connect is not None and not self._pending_connect.is_set()

    def disconnect_start(self) -> asyncio.Task:
        """Create an event to be set by the disconnect callback, and return a
        task that wait for it."""
        self._pending_disconnect = asyncio.Event()
        return asyncio.create_task(self._pending_disconnect.wait(), name="disconnected")

    def disconnect_done(self):
        """Called when the disconnection is done"""
        if self._pending_disconnect is not None:
            self._loop.call_soon_threadsafe(self._pending_disconnect.set)

    def has_pending_disconnect(self) -> bool:
        """Return true if a disconnection is pending"""
        return self._pending_disconnect is not None and not self._pending_disconnect.is_set()

    def publish_start(self, mid: int) -> asyncio.Task:
        """Create an event to be set when the publish is done and return a
        task that wait for it."""
        self._pending_publish[mid] = asyncio.Event()
        return asyncio.create_task(self._pending_publish[mid].wait(), name="published")

    def publish_done(self, key: int) -> None:
        """Set the event corresponding to a publish, and remove it from the
        pending publish"""
        if key in self._pending_publish:
            ev = self._pending_publish.pop(key)
            ev.set()

    def subcribe_start(self, mid: int) -> asyncio.Task:
        """Create a new event, and wait for it."""
        self._pending_subscribe[mid] = asyncio.Event()
        return asyncio.create_task(
            self._pending_subscribe[mid].wait(), name="subscribed"
        )

    def subscribe_done(self, key: int) -> None:
        """Set the event corresponding to this subscription, and remove it
        from the pending subscriptions."""
        if key in self._pending_subscribe:
            ev = self._pending_subscribe.pop(key)
            self._loop.call_soon_threadsafe(ev.set)
            ev.set()

    def unsubcribe_start(self, mid: int) -> asyncio.Task:
        """Create a new event, and wait for it."""
        self._pending_unsubscribe[mid] = asyncio.Event()
        return asyncio.create_task(
            self._pending_unsubscribe[mid].wait(), name="unsubscribed"
        )

    def unsubscribe_done(self, key: int) -> None:
        """Set the event corresponding to this subscription, and remove it from the
        pending subscriptions."""
        if key in self._pending_unsubscribe:
            ev = self._pending_unsubscribe.pop(key)
            self._loop.call_soon_threadsafe(ev.set)

    def put_msg(self, message):
        """Add a message in the queue"""
        found = False
        for sub in self._msg_queues:
            if topic_matches_sub(sub, message.topic):
                self._msg_queues[sub].put_nowait(message)
                found = True
        if not found:
            logger.warning(
                f"Warning: message on topic {message.topic} did not match any subscription"
            )

    async def get_msg(self, topic):
        """Wait for a message to arrive on a subscribed topic"""
        if topic in self._msg_queues:
            return await self._msg_queues[topic].get()
        else:
            logger.error(f"Must subscribe to topic {topic} first")
            return None

    def add_topic(self, topic):
        """Add a topic in the message queues"""
        self._msg_queues[topic] = asyncio.Queue()

    def del_topic(self, topic):
        """Remove a topic from the message queue."""
        if topic in self._msg_queues:
            del self._msg_queues[topic]


# MQTT callbacks


def on_mqtt_connect(
    client: mqtt.Client, userdata: MqttQueues, _flags, reason_code, _properties
):
    """Callback when a SUBACK is received"""
    if not userdata.has_pending_connect():
        return
    if reason_code.is_failure:
        logger.debug(f"Failed to connect: {reason_code}.")
    else:
        # we should always subscribe from on_connect callback to be
        # sure our subscribed is persisted across reconnections.
        userdata.connect_done()
        logger.debug("connected.")
        for topic in userdata.topic_list():
            client.subscribe(topic)


def on_mqtt_disconnect(
    _client: mqtt.Client,
    userdata: MqttQueues,
    _flags: mqtt.DisconnectFlags,
    _reason_code: ReasonCode,
    _properties: Properties | None = None,
) -> None:
    """Callback called when a UNSUBACK is received"""
    if not userdata.has_pending_disconnect():
        logger.warning("unexpected disconnection")
        return
    userdata.disconnect_done()


def on_mqtt_message(_client: mqtt.Client, userdata: MqttQueues, message):
    """Callback for message reception"""
    userdata.put_msg(message)


def on_mqtt_subscribe(
    _client: mqtt.Client,
    userdata: MqttQueues,
    mid: int,
    _reason_codes: list[ReasonCode],
    _properties: Properties | None = None,
) -> None:
    """Callback for subcriptions"""
    userdata.subscribe_done(mid)


def on_mqtt_unsubscribe(
    _client: mqtt.Client,
    userdata: MqttQueues,
    mid: int,
    _reason_codes: list[ReasonCode],
    _properties: Properties | None = None,
):
    """Callback called when a UNSUBACK is received"""
    userdata.unsubscribe_done(mid)


def on_mqtt_publish(
    _client: mqtt.Client,
    userdata: MqttQueues,
    mid: int,
    _reason_code: ReasonCode,
    _properties: Properties,
):
    """Get and trigger (set) the event."""
    userdata.publish_done(mid)


class MqttMixinError(BaseException):
    """Exceptions generated from the mqtt mixin"""

    def __init__(self, rc: int | None, *args: Any):
        super().__init__(*args)
        self.rc = rc

    def __str__(self):
        return f"[code:{self.rc.value}] {self.rc!s}"


class MqttMixin:
    """
    This mixin provides MQTT support to SPADE agents.
    It must be used as superclass of a spade.Agent subclass.
    """

    async def _hook_plugin_after_connection(self, *args, **kwargs):
        try:
            await super()._hook_plugin_after_connection(*args, **kwargs)
        except AttributeError:
            logger.debug("_hook_plugin_after_connection is undefined")
        # create a new mqtt client
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
        # add an instance of the mqtt component to the agent
        self.mqtt = MqttComponent(client)

    async def _hook_plugin_before_connection(self, *args, **kwargs):
        """
        Overload this method to hook a plugin before connetion is done
        """
        try:
            await super()._hook_plugin_before_connection(*args, **kwargs)
        except AttributeError:
            logger.debug("_hook_plugin_before_connection is undefined")


class MqttComponent:
    """Mqtt client wrapper."""

    def __init__(self, client: mqtt.Client):
        self.clients = [client]
        self.queues = MqttQueues(asyncio.get_event_loop())
        self.init_client(client)

    def init_client(self, client):
        client.on_connect = on_mqtt_connect
        client.on_disconnect = on_mqtt_disconnect
        client.on_message = on_mqtt_message
        client.on_publish = on_mqtt_publish
        client.on_subscribe = on_mqtt_subscribe
        client.on_unsubscribe = on_mqtt_unsubscribe
        client.user_data_set(self.queues)

    def add_client(self):
        """create a new mqtt client and append it to the list of clients"""
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, protocol=mqtt.MQTTv5)
        self.clients.append(client)
        self.init_client(client)
        return client

    async def connect(
        self, broker, timeout, username=None, password=None, append=False, **kwargs
    ):
        """Connect to the MQTT broker (and start the loop). If another
        connection is done, create a new client and a new event loop"""
        if not append and self.clients[0].is_connected():
            raise RuntimeWarning("warning, client already connected")
        elif append:
            # try creating a new client and event loop
            client = self.add_client()
        else:
            client = self.clients[0]
        client.username = username
        client.password = password
        logger.debug(f" connecting to {broker}...")
        client.connect(broker, **kwargs)
        connected_task = self.queues.connect_start()
        client.loop_start()
        if self.queues.has_pending_connect():
            await asyncio.wait_for(connected_task, timeout=timeout)

    async def disconnect(self):
        """Disconnect from the MQTT broker (and stop the loop)"""
        for client in self.clients:
            client.disconnect()
            disconnected_task = self.queues.disconnect_start()
            await asyncio.wait_for(disconnected_task, timeout=5)
            client.loop_stop()

    def is_connected(self, client_id=0):
        """Check connection status"""
        return self.clients[client_id].is_connected()

    # SUBSCRIBER USE CASES
    async def subscribe(self, topic_name: str, client_id=0):
        """
        Subscribe to a topic.

        Args:
            topic: name of the topix to subscribe to
        """
        # add a queue for this topic
        self.queues.add_topic(topic_name)
        # if connected, subscribe immediately
        if self.clients[client_id].is_connected():
            logger.debug(f"subscribing to topic {topic_name}")
            result, mid = self.clients[client_id].subscribe(topic_name)
            if result != mqtt.MQTT_ERR_SUCCESS or mid is None:
                raise MqttMixinError(result, "Could not subscribe to topic")

            subscribed_task = self.queues.subcribe_start(mid)
            await asyncio.wait_for(subscribed_task, timeout=30)
        else:
            raise MqttMixinError("Could not subscribe to topic: not connected")

    async def receive(self, topic):
        """Asynchronously receive a message from a topic"""
        return await self.queues.get_msg(topic)

    async def unsubscribe(self, topic_name: str, client_id = 0):
        """
        Unsubscribe from a topic.

        Args:
            topic_name (str): name of the mqtt topic.
        """
        if topic_name in self.queues.topic_list():
            logger.debug(f"unsubscribing from topic {topic_name}")
            result, mid = self.clients[client_id].unsubscribe(topic_name)
            if result != mqtt.MQTT_ERR_SUCCESS or mid is None:
                raise MqttMixinError("Could not unsubscribe from topic")
            unsubscribed_task = self.queues.unsubcribe_start(mid)
            await asyncio.wait_for(unsubscribed_task, timeout=5)
            self.queues.del_topic(topic_name)
        else:
            logger.error(f"Error unsubscribing from topic {topic_name}")

    # PUBLISHER USE CASES
    async def publish(
        self,
        topic: str,
        payload: str,
        qos=0,
        properties=None,
        retain=False,
        client_id=0,
    ):
        """
        Publish an item to a node.

        Args:
            topic (str): Name of the topic.

        """
        props = Properties(packetType=PacketTypes.PUBLISH)
        if properties:
            for k, v in properties.items():
                props.UserProperty = (k, v)

        info = self.clients[client_id].publish(
            topic=topic, payload=payload, qos=qos, retain=retain, properties=props
        )
        # Early out on error
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            raise MqttMixinError(info.rc, "Could not publish message")
        # Early out on immediate success
        if info.is_published():
            return
        pub_task = self.queues.publish_start(info.mid)
        await asyncio.wait_for(pub_task, timeout=5)
