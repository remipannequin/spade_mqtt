import asyncio

import spade
from spade_mqtt.mqtt_plugin import MqttMixin
from helpers import PingerAgent, PingReply


class MQTTSub(spade.behaviour.CyclicBehaviour):
    subscribed = False
    received = 0

    def __init__(
        self, parent: "MqttSubAgent", topic, limit: int | float = float("inf")
    ):
        super().__init__()
        self.parent = parent
        self.topic = topic
        self.limit = limit

    async def run(self) -> None:
        assert self.agent and self.agent.mqtt
        msg = await self.agent.mqtt.receive(self.topic)
        self.parent.record(self.topic, msg)
        self.received += 1
        if self.received >= self.limit:
            self.kill()

    async def on_start(self) -> None:
        assert self.agent and self.agent.mqtt
        assert self.agent.mqtt.is_connected()
        await self.agent.mqtt.subscribe(self.topic)
        self.subscribed = True

    async def on_end(self) -> None:
        assert self.agent and self.agent.mqtt
        # this fails
        await self.agent.mqtt.unsubscribe(self.topic)
        await self.agent.mqtt.disconnect()


class MqttSubAgent(MqttMixin, spade.agent.Agent):
    sub_bh: list[MQTTSub]
    connect_done = False
    mqtt_port: int
    mqtt_host: str
    received: dict[str, list]

    def __init__(
        self,
        jid,
        pwd,
        mqtt_host,
        mqtt_port,
        topics: list[tuple[str, int] | str],
        port=5222,
    ):
        super().__init__(jid=jid, password=pwd, port=port)
        self.mqtt_host = mqtt_host
        self.mqtt_port = mqtt_port
        self.topics = topics
        self.sub_bh = []
        self.received = {}

    async def setup(self) -> None:
        self.add_behaviour(PingReply())
        await self.mqtt.connect(broker=self.mqtt_host, port=self.mqtt_port, timeout=1)
        assert self.mqtt.is_connected()
        self.connect_done = True
        for e in self.topics:
            if isinstance(e, tuple):
                bh = MQTTSub(self, *e)
                self.received[e[0]] = []
            else:
                bh = MQTTSub(self, e)
                self.received[e] = []
            self.sub_bh.append(bh)
            self.add_behaviour(bh)

    def record(self, topic, msg):
        self.received[topic].append(msg)


def test_basic_agent_start(spade_broker):
    """Basic test to check that the fixture works as expected"""

    success = False

    class TestBehaviour(spade.behaviour.OneShotBehaviour):
        async def run(self):
            assert self.agent
            self.agent.prop = True

    class TestAgent(spade.agent.Agent):
        prop = False
        test_bh: TestBehaviour

        async def setup(self) -> None:
            self.test_bh = TestBehaviour()
            self.add_behaviour(self.test_bh)

    async def start_ag1(xmpp_host, xmpp_port):
        """Launch one testing agent."""
        nonlocal success
        t1 = TestAgent("@".join(["test1", xmpp_host]), "pwd", port=xmpp_port)
        await t1.start(auto_register=True)  # use host and port
        assert t1.test_bh
        while not t1.test_bh.is_killed():
            try:
                await asyncio.sleep(1)
            except KeyboardInterrupt:
                break
        assert t1.prop
        success = True
        await asyncio.sleep(1)

    host, port = spade_broker

    spade.container.Container().reset()  # type: ignore
    spade.run(start_ag1(host, port))
    assert success


def test_sub_one(spade_broker, mosquitto_container):
    """Test 1: connect to one broker on a defined topic
    external client send messages on topic
    agent receive messages.
    another agent send "ping" message to check that
    agent undertest is right"""

    async def start_ag1(xmpp_host, xmpp_port, mqtt_host, mqtt_port, pub_fn):
        """Launch one testing agent."""
        nonlocal success
        t1 = MqttSubAgent(
            "test1@" + xmpp_host,
            "pwd",
            mqtt_host,
            mqtt_port,
            topics=[("test/topic1", 2)],
            port=xmpp_port,
        )
        await t1.start(auto_register=True)  # use host and port
        # start pinger agent
        pinger = PingerAgent(
            "pinger@" + xmpp_host, "pwd", "test1@" + xmpp_host, period=1, port=xmpp_port
        )
        await pinger.start(auto_register=True)
        assert len(t1.sub_bh) == 1
        assert t1.sub_bh[0]
        assert t1.connect_done
        await pub_fn(5, "test/topic1", "hello1")
        await pub_fn(5, "test/topic1", "hello2")
        while not t1.sub_bh[0].is_killed():
            try:
                await asyncio.sleep(1)
            except KeyboardInterrupt:
                break
        assert len(t1.received["test/topic1"]) == 2
        assert t1.received["test/topic1"][0].payload == b"hello1"
        assert t1.received["test/topic1"][1].payload == b"hello2"
        assert pinger.all_replied()

        success = True

    host, port = spade_broker
    mqtt_host, mqtt_port, fn = mosquitto_container
    success = False
    # Reset spade container (because previous test closed the event loop)
    spade.container.Container().reset()  # type: ignore
    spade.run(
        start_ag1(
            xmpp_host=host,
            xmpp_port=port,
            mqtt_host=mqtt_host,
            mqtt_port=mqtt_port,
            pub_fn=fn,
        ),
        embedded_xmpp_server=False
    )
    assert success


def test_sub_wildcard(spade_broker, mosquitto_container):
    """Test 2: same as one on a wildcard topic"""

    async def start_ag1(xmpp_host, xmpp_port, mqtt_host, mqtt_port, pub_fn):
        """Launch one testing agent."""
        nonlocal success
        t1 = MqttSubAgent(
            "test1@" + xmpp_host,
            "pwd",
            mqtt_host,
            mqtt_port,
            topics=[("test/#", 2)],
            port=xmpp_port,
        )
        await t1.start(auto_register=True)  # use host and port
        # start pinger agent
        pinger = PingerAgent(
            "pinger@" + xmpp_host, "pwd", "test1@" + xmpp_host, period=1, port=xmpp_port
        )
        await pinger.start(auto_register=True)
        assert len(t1.sub_bh) == 1, "more than one behaviour in agent"
        assert t1.sub_bh[0]
        assert t1.connect_done
        await pub_fn(5, "test/topic1", "hello1")
        await pub_fn(5, "test/topic1", "hello2")
        while not t1.sub_bh[0].is_killed():
            try:
                await asyncio.sleep(1)
            except KeyboardInterrupt:
                break
        assert len(t1.received["test/#"]) == 2
        assert t1.received["test/#"][0].payload == b"hello1"
        assert t1.received["test/#"][1].payload == b"hello2"
        assert pinger.all_replied()

        success = True

    host, port = spade_broker
    mqtt_host, mqtt_port, fn = mosquitto_container
    success = False
    # Reset spade container (because previous test closed the event loop)
    spade.container.Container().reset()  # type: ignore
    spade.run(
        start_ag1(
            xmpp_host=host,
            xmpp_port=port,
            mqtt_host=mqtt_host,
            mqtt_port=mqtt_port,
            pub_fn=fn,
        )
    )
    assert success





# Test 3: same as one, but with two brokers

# Test 4: agent publish on a defined topic
# external tool gets the messages


# Test 5: publish while connected to two MQTT brokers
