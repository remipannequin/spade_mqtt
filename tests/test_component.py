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
    cid = await c.connect(broker=host, port=port, timeout=2)
    assert cid == 0
    assert c.is_connected()
    await asyncio.sleep(2)
    await c.disconnect()
    assert not c.is_connected()


@pytest.mark.asyncio
async def test_connect_already_connected(mosquitto_container):
    """Test connecting when already connected. raise RuntimeWarning"""
    c = MqttComponent()
    assert not c.is_connected()
    host, port, pub_fn = mosquitto_container
    await c.connect(broker=host, port=port, timeout=2)
    with pytest.raises(RuntimeWarning):
        await c.connect(broker=host, port=port, timeout=2)


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
    with pytest.raises(RuntimeWarning):
        await c.receive("test/topic1")


@pytest.mark.asyncio
async def test_sub_fail2():
    c = MqttComponent()
    # a warning is displayed
    with pytest.raises(RuntimeWarning):
        await c.receive("test/topic1")


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

@pytest.mark.asyncio
async def test_pub_prop(mosquitto_container):
    c = MqttComponent()
    host, port, _ = mosquitto_container
    await c.connect(broker=host, port=port, timeout=3)
    await c.subscribe("test/topic1")
    await c.publish("test/topic1", "hello1", properties={"p1": "1", "p2": "string"})
    await asyncio.sleep(1)
    m = await c.receive("test/topic1")
    assert m
    assert m.payload == b"hello1"
    assert m.properties and m.properties.UserProperty # type: ignore
    d = dict(m.properties.UserProperty) # type: ignore
    assert "p1" in d and "p2" in d
    assert d["p1"] == "1"
    assert d["p2"] == "string"
    await c.unsubscribe("test/topic1")
    await c.disconnect()


@pytest.mark.asyncio
async def test_pub_fail(mosquitto_container):
    c = MqttComponent()
    host, port, pub_fn = mosquitto_container
    with pytest.raises(MqttMixinError):
        await c.publish("test/topic1", "hello1")
   

@pytest.mark.asyncio
async def test_sub_multiple(mosquitto_container, mosquitto_container2):
    c = MqttComponent()
    assert not c.is_connected()
    host, port, pub_fn = mosquitto_container
    client_1 = await c.connect(broker=host, port=port, timeout=2)
    host2, port2, pub_fn2 = mosquitto_container2
    client_2 = await c.connect(broker=host2, port=port2, timeout=2, append=True)
    assert len(c.clients) == 2
    assert client_2 == 1
    # subscribe to a topic on each client
    await c.subscribe("test/topic1", client_id=client_1)
    await c.subscribe("test/topic2", client_id=client_2)
    await pub_fn(0.5, "test/topic1", "test11")
    await pub_fn(1.2, "test/topic2", "test12")
    await pub_fn2(1.1, "test/topic1", "test21")
    await pub_fn2(1.7, "test/topic2", "test22")
    m1 = await c.receive("test/topic1")
    m2 = await c.receive("test/topic2")
    assert m1 and m2
    assert m1.payload == b'test11'
    assert m2.payload == b'test22'


@pytest.mark.asyncio
async def test_multiple_pub(mosquitto_container, mosquitto_container2):
    c = MqttComponent()
    host, port, _ = mosquitto_container
    cl1 = await c.connect(broker=host, port=port, timeout=3)
    await c.subscribe("test/topic1", client_id=cl1)
    host2, port2, _ = mosquitto_container2
    cl2 = await c.connect(broker=host2, port=port2, timeout=3, append=True)
    await c.subscribe("test/topic2", client_id=cl2)

    await c.publish("test/topic1", "hello1", client_id=cl1)
    await c.publish("test/topic2", "hello2", client_id=cl2)
    await asyncio.sleep(1)
    m1 = await c.receive("test/topic1")
    assert m1
    assert m1.payload == b"hello1"
    m2 = await c.receive("test/topic2")
    assert m2
    assert m2.payload == b"hello2"

    await c.unsubscribe("test/topic1", client_id=cl1)
    await c.unsubscribe("test/topic2", client_id=cl2)
    await c.disconnect()