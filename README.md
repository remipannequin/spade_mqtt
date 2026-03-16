# spade_mqtt

The goal of this project is to implement a way to enable spade agents to interact
(connect, subscribe, publish, ...) on MQTT.

## Installation

```
pip install spade_mqtt
```

## Getting started

Agent should use it by adding the MqttMixin in the class declaration.
This mixin add the mqtt attribute in the agent class. This object wraps
mqtt calls and manage asyncio.


```python
class MyAgent(MqttMixin, spade.agent.Agent):

    async def setup(self):
        try:
            await self.mqtt.connect("localhost", 30)
        except TimeoutError:
            print("Warning: could not connect to MQTT brocker localhost")
        #...
```

Such agent can then have behaviour that can subscribe on topics, and await messages
on them.

```python
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
            # process the message...

    async def on_start(self):
        await self.agent.mqtt.subscribe(self.topic)

    async def on_end(self):
        await self.agent.mqtt.unsubscribe(self.topic)

```

MQTT publish is done likewise.

```python
class MQTTPubBehaviour(spade.behaviour.OneShotBehaviour):
    """Publish hello world to example/topic"""

    async def run(self):
        await self.agent.mqtt.publish(
            "example/topic", "hello world", retain=True
        )
```

