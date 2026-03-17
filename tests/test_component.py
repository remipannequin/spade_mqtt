from spade_mqtt.mqtt_plugin import MqttComponent, MqttMixinError

import asyncio
import pytest


def test_create(mosquitto_container):
    """Check that connecting to a non existant server raise the right exception"""
    c = MqttComponent()
    assert c.clients
    assert len(c.clients) == 1
    assert c.clients[0]


@pytest.mark.asyncio
async def test_connect(mosquitto_container):
    """Test connecting and disconnecting from broker"""
    c = MqttComponent()
    assert not c.is_connected()
    host, port, pub_fn = mosquitto_container
    await c.connect(broker=host, port=port, timeout=2)
    assert c.is_connected()
    await asyncio.sleep(2)
    await c.disconnect()
    assert not c.is_connected()


@pytest.mark.asyncio
async def test_connect_exception(mosquitto_container):
    """Check that connecting to a non existant server raise the right exception"""
    c = MqttComponent()
    with pytest.raises(ConnectionRefusedError):
        await c.connect(broker="localhost", port=100000, timeout=1)


@pytest.mark.asyncio
async def test_sub(mosquitto_container):
    c = MqttComponent()
    host, port, pub_fn = mosquitto_container
    await c.connect(broker=host, port=port, timeout=2)
    await c.subscribe("test/topic1")
    await pub_fn(1, "test/topic1", "hello1")
    m = await c.receive("test/topic1")
    assert m
    assert m.payload == b"hello1"
    await c.unsubscribe("test/topic1")
    await c.disconnect()


@pytest.mark.asyncio
async def test_sub_fail(mosquitto_container):
    c = MqttComponent()
    host, port, pub_fn = mosquitto_container
    await c.connect(broker=host, port=port, timeout=2)
    # a warning is displayed, None is returned
    m = await c.receive("test/topic1")
    assert m is None


@pytest.mark.asyncio
async def test_sub_wildcard(mosquitto_container):
    c = MqttComponent()
    host, port, pub_fn = mosquitto_container
    await c.connect(broker=host, port=port, timeout=2)
    await c.subscribe("test/#")
    await pub_fn(1, "test/topic1", "hello1")
    m = await c.receive("test/#")
    assert m
    assert m.payload == b"hello1"
    await c.unsubscribe("test/#")
    await c.disconnect()


@pytest.mark.asyncio
async def test_pub(mosquitto_container):
    c = MqttComponent()
    host, port, pub_fn = mosquitto_container
    await c.connect(broker=host, port=port, timeout=3)
    await c.subscribe("test/topic1")
    await c.publish("test/topic1", "hello1")
    await asyncio.sleep(1)
    m = await c.receive("test/topic1")
    assert m
    assert m.payload == b"hello1"
    await c.unsubscribe("test/topic1")
    await c.disconnect()