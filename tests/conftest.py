
import time
import subprocess
import signal
import asyncio
import pytest

from testcontainers.core.container import DockerContainer


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
    print("Starting spade XMPP server")
    proc = subprocess.Popen(["spade", "run", "--host", "127.0.0.1", "--memory"])
    time.sleep(2)
    yield "localhost", 5222
    print("closing Spade XMPP server")
    proc.send_signal(signal.SIGINT)
    proc.wait()
    print("Spade XMPP server closed")


