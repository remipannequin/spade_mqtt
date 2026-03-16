# use testcontainers to
# - start a MQTT broker (eclipse-mosquitto)
# - start a XMPP broker (spade run)
import time
import subprocess
import signal
import asyncio

import pytest

from datetime import datetime as dt, timedelta
from testcontainers.core.container import DockerContainer

import spade
from spade_mqtt.mqtt_plugin import MqttMixin


@pytest.fixture(scope="session")
def mosquitto_container():
    container = DockerContainer("eclipse-mosquitto").with_exposed_ports(1883)
    container.start()

    host = container.get_container_host_ip()
    port = container.get_exposed_port(1883)

    async def delayed_pub(delay, topic, payload):
        await asyncio.sleep(delay)
        container.exec(["mosquitto_pub", "-h", "localhost", "-t", topic, "-m", payload])

    yield host, port, delayed_pub

    container.stop()


@pytest.fixture(scope="session")
def spade_broker():
    proc = subprocess.Popen(["spade", "run", "--host", "127.0.0.1", "--memory"])
    time.sleep(2)
    yield "localhost", 5222

    proc.send_signal(signal.SIGINT)
    proc.wait()


def test_basic_agent_start(spade_broker):
    """Basic test to check that the fixture works as expected"""

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

    async def start_ag1():
        """Launch one testing agent."""
        t1 = TestAgent("test1@localhost", "pwd")
        await t1.start(auto_register=True)  # use host and port
        assert t1.test_bh
        while not t1.test_bh.is_killed():
            try:
                await asyncio.sleep(1)
            except KeyboardInterrupt:
                break
        assert t1.prop

    host, port = spade_broker
    spade.run(start_ag1())


def test_sub_one(spade_broker, mosquitto_container):
    """Test 1: connect to one broker on a defined topic
    external client send messages on topic
    agent receive messages.
    another agent send "ping" message to check that
    agent undertest is right"""

    class PingReply(spade.behaviour.CyclicBehaviour):
        async def run(self):
            m = await self.receive(1)
            if m and m.body and "ping" in m.body:
                r = m.make_reply()
                r.body = "pong"
                await self.send(r)

    class Ping(spade.behaviour.PeriodicBehaviour):
        replied: list[bool] = []
        sent_date: list[dt] = []
        delay: list[timedelta|None] = []
        ping_num = 0

        def __init__(self, period, to):
            super().__init__(period=period)
            self.to = to

        async def run(self) -> None:
            assert self.agent
            thread_id = f"ping-{dt.now().timestamp()}"
            m = spade.message.Message(to=self.to, body="ping", thread=thread_id)
            template = spade.template.Template(thread=thread_id)
            await self.send(m)
            self.sent_date.append(dt.now())
            self.delay.append(None)
            self.replied.append(False)
            self.agent.add_behaviour(PingWaitReply(self, self.ping_num), template)
            self.ping_num += 1

        def record_reply(self, index):
            if index >= len(self.replied):
                raise RuntimeError("Got a reply for non-existant ping")
            self.replied[index] = True
            self.delay[index] = dt.now() - self.sent_date[index]

        def all_received(self):
            """Check if all ping requests have been replied"""
            return all(self.replied)

        def average_delay(self):
            """Get the average delay in microseconds"""
            valid_delays = [d.microseconds for d in self.delay if d]
            return sum(valid_delays) / len(valid_delays)

    class PingWaitReply(spade.behaviour.CyclicBehaviour):
        def __init__(self, parent: Ping, index: int):
            super().__init__()
            self.parent = parent
            self.index = index

        async def run(self):
            m = await self.receive(1)
            if m:
                assert m.body and m.body == "pong"
                self.parent.record_reply(self.index)

    class MQTTSub(spade.behaviour.CyclicBehaviour):
        subscribed = False
        received = []

        async def run(self) -> None:
            assert self.agent
            assert self.agent.mqtt
            if not self.subscribed:
                await self.agent.mqtt.subscribe("test/topic1")
                self.subscribed = True
            msg = await self.agent.mqtt.receive("test/topic1")
            self.received.append(msg)
            # Try to get only 2 messages
            if len(self.received) >= 2:
                self.kill()

    class PingerAgent(spade.agent.Agent):
        ping_bh: Ping

        async def setup(self) -> None:
            self.ping_bh = Ping(1, "test1@localhost")
            self.add_behaviour(self.ping_bh)

    class TestAgent(MqttMixin, spade.agent.Agent):
        test_bh: MQTTSub
        connect_done = False
        mqtt_port: int
        mqtt_host: str

        def __init__(self, jid, pwd, mqtt_host, mqtt_port):
            super().__init__(jid=jid, password=pwd)
            self.mqtt_host = mqtt_host
            self.mqtt_port = mqtt_port

        async def setup(self) -> None:
            self.add_behaviour(PingReply())
            await self.mqtt.connect(
                broker=self.mqtt_host, port=self.mqtt_port, timeout=1
            )
            assert self.mqtt.is_connected()
            self.connect_done = True
            self.test_bh = MQTTSub()
            self.add_behaviour(self.test_bh)

    async def start_ag1(mqtt_host, mqtt_port, pub_fn):
        """Launch one testing agent."""
        nonlocal success
        t1 = TestAgent("test1@localhost", "pwd", mqtt_host, mqtt_port)
        await t1.start(auto_register=True)  # use host and port
        # start pinger agent
        pinger = PingerAgent("pinger@localhost", "pwd")
        await pinger.start(auto_register=True)
        assert t1.test_bh
        assert t1.connect_done
        await pub_fn(5, "test/topic1", "hello1")
        await pub_fn(5, "test/topic1", "hello2")
        while not t1.test_bh.is_killed():
            try:
                await asyncio.sleep(1)
            except KeyboardInterrupt:
                break
        assert len(t1.test_bh.received) == 2
        assert t1.test_bh.received[0].payload == b"hello1"
        assert t1.test_bh.received[1].payload == b"hello2"
        assert all(pinger.ping_bh.replied)

        success = True

    host, port = spade_broker
    success = False
    spade.run(start_ag1(*mosquitto_container))
    assert success


# Test 2: same as one on a wildcard topic

# Test 3: same as one, but with two brokers

# Test 4: agent publish on a defined topic
# external tool gets the messages



# Test 5: publish while connected to two MQTT brokers
