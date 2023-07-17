#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: server
@time: 2023/2/17
"""

import asyncio
from typing import Optional, Union, Dict, Tuple, Any, Set, Callable, ByteString

from pyraft.state import State, Follower, Candidate, Leader
from pyraft.network import UDPProtocol
from pyraft.storage import StateMachine


class Server:
    servers = set()

    def __init__(self, host: str, port: int, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.host = host
        self.port = port
        self.loop = loop or asyncio.get_event_loop()
        self.cluster: Set[Tuple[str, int]] = set()
        self.requests = asyncio.Queue()
        self.state: Optional[State] = None
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.__class__.servers.add(self)

    async def start(self):
        protocol = UDPProtocol(self.requests, self.request_handler, loop=self.loop)
        self.transport, _ = await self.loop.create_datagram_endpoint(protocol, local_addr=(self.host, self.port))

        def start_state():
            self.state = State(self)
            self.state.start()

        self.loop.call_soon(start_state)

    def stop(self):
        self.state.stop()
        self.transport.close()

    def update_cluster(self, host: str, port: int):
        self.cluster.add((host, port))

    def request_handler(self, data: ByteString, sender: Tuple[str, int]) -> None:
        self.state.request_handler(data, sender)

    async def send(self, data: ByteString, dest: Union[str, Tuple[str, int]]):
        if isinstance(dest, str):
            host, port = dest.split(':')
            dest = (host, int(port))
        await self.requests.put((data, dest))

    async def broadcast(self, data: ByteString):
        tasks = [self.send(data, dest) for dest in self.cluster]
        await asyncio.gather(*tasks)

    @staticmethod
    def add_state_apply_handler(handler: Optional[Callable[[StateMachine, Dict[str, Any]], None]]):
        State.add_state_apply_handler(handler)

    @staticmethod
    def add_follower_listener(callback: Callable[['Follower'], None]):
        State.add_follower_listener(callback)

    @staticmethod
    def add_candidate_listener(callback: Callable[['Candidate'], None]):
        State.add_candidate_listener(callback)

    @staticmethod
    def add_leader_listener(callback: Callable[['Leader'], None]):
        State.add_leader_listener(callback)
