
from datetime import datetime as dt, timedelta

import spade


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


class PingerAgent(spade.agent.Agent):
    """Agent that periodicaly sends message to a target, and record the time to get a reply."""
    ping_bh: Ping
    target: str
    period: int

    def __init__(self, jid: str, password: str, target: str, port: int = 5222, period = 1):
        super().__init__(jid, password, port)
        self.target = target
        self.period = period

    async def setup(self) -> None:
        self.ping_bh = Ping(self.period, self.target)
        self.add_behaviour(self.ping_bh)

    def all_replied(self) -> bool:
        return self.ping_bh.all_received()
    

