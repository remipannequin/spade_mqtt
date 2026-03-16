"""Demonstrate the MQTT plugin for spade.

A mqtt broker should be running, as well as a XMPP server.

"""

import asyncio
import spade
from spade_mqtt.mqtt_plugin import MqttMixin


class MQTTSubBehaviour(spade.behaviour.CyclicBehaviour):
    """Behaviour that subscribes to a topic and store the messages received."""

    def __init__(self, topic):
        spade.behaviour.CyclicBehaviour.__init__(self)
        self.topic = topic

    async def run(self):
        print(f"awaiting message on topic {self.topic}")
        msg = await self.agent.mqtt.receive(self.topic)
        if msg:
            print(f"got message with payload {msg.payload}")
            self.agent.last_msg = msg.payload.decode("utf-8")

    async def on_start(self):
        await self.agent.mqtt.subscribe(self.topic)

    async def on_end(self):
        await self.agent.mqtt.unsubscribe(self.topic)


class MQTTPubBehaviour(spade.behaviour.PeriodicBehaviour):
    """Increment a counter every second, and publish it to a mqtt topic."""

    def __init__(self, topic):
        spade.behaviour.PeriodicBehaviour.__init__(self, 1)
        self.topic = topic

    async def on_start(self):
        self.agent.t = 0

    async def run(self):
        self.agent.t += 1
        if self.agent.t >= 10:
            await self.agent.stop()
        m = f"tick {self.agent.t}"
        print(f"agent {self.agent.jid} publishing {m} on topic {self.topic}")
        await self.agent.mqtt.publish(
            self.topic, m, properties={"performative": "inform", "thread": "test"}, retain=True
        )


class PubSubAgent(MqttMixin, spade.agent.Agent):
    """Testing agent"""

    t: int = -1
    mqtt_behav: MQTTSubBehaviour = None
    last_msg: str = None

    async def setup(self):
        print(f"Agent {self.jid} starting...")
        try:
            await self.mqtt.connect("localhost", 30)
        except TimeoutError:
            print("Warning: could not connect to MQTT brocker localhost")
        self.mqtt_behav = MQTTSubBehaviour("test/topic1")
        self.add_behaviour(self.mqtt_behav)
        self.add_behaviour(MQTTPubBehaviour("test/topic1"))


def pub_agent():
    """Fixture that create an agent that periodically publish data"""

    class PubAg(MqttMixin, spade.agent.Agent):
        """Agent that publish to mqtt"""
        async def setup(self):
            self.mqtt.connect("localhost", 5)
            self.add_behaviour(MQTTPubBehaviour("test/topic1"))

    return PubAg("pub@localhost", "pwd")


async def start_ag1():
    """Launch one testing agent."""
    t1 = PubSubAgent("test1@localhost", "pwd")
    await t1.start(auto_register=True)
    # wait until behaviour is killed
    while not t1.mqtt_behav.is_killed():
        try:
            await asyncio.sleep(1)
        except KeyboardInterrupt:
            break
    await asyncio.sleep(1)
    await t1.mqtt.disconnect()
    await t1.stop()
    
    assert t1.t == 10, "publish behaviour did not run as expected"
    assert t1.last_msg == "tick 10", "subscribe behaviour did not run as expected"



def test_pubsub():
    """Test an agent that both publish and subscribe."""
    spade.run(start_ag1())

if __name__ == "__main__":
    spade.run(start_ag1())
