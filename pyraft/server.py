#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: server
@time: 2023/2/17
"""

import asyncio
from typing import Optional, Union, Tuple, Set, Callable

from pyraft.state import State, Follower, Candidate, Leader
from pyraft.network import UDPProtocol


class Server:
    servers = set()

    def __init__(self, addr: Tuple[str, int], loop: Optional[asyncio.AbstractEventLoop] = None):
        self.addr = addr
        self.loop = loop or asyncio.get_event_loop()
        self.cluster: Set[Tuple[str, int]] = set()
        self.state = State(self)
        self.udp_protocol: Optional[UDPProtocol] = None
        self.udp_transport: Optional[asyncio.DatagramTransport] = None
        self.__class__.servers.add(self)

    async def start(self):
        await self.create_udp_endpoint(self.addr)
        self.loop.call_soon(self.state.start)

    def stop(self):
        self.state.stop()
        self.udp_transport.close()

    def update_cluster(self, addr: Tuple[str, int]):
        self.cluster.add(addr)

    def request_handler(self, data: bytes, sender: Tuple[str, int]) -> None:
        self.state.request_handler(data, sender)

    async def create_udp_endpoint(self, addr: Tuple[str, int]):
        async def transport_listener():
            try:
                await on_con_lost
            finally:
                self.udp_transport.close()
                await self.create_udp_endpoint(addr)

        on_con_lost = asyncio.Future(loop=self.loop)
        self.udp_protocol = UDPProtocol(self.request_handler, on_con_lost, loop=self.loop)
        self.udp_transport, _ = await self.loop.create_datagram_endpoint(self.udp_protocol, local_addr=addr)
        asyncio.ensure_future(transport_listener(), loop=self.loop)

    def send(self, data: bytes, dest: Union[str, Tuple[str, int]]):
        if isinstance(dest, str):
            host, port = dest.split(':')
            dest = (host, int(port))
        self.udp_protocol.send(data, dest)

    def broadcast(self, data: bytes):
        [self.send(data, dest) for dest in self.cluster]

    @staticmethod
    def add_follower_listener(callback: Callable[['Follower'], None]):
        State.add_follower_listener(callback)

    @staticmethod
    def add_candidate_listener(callback: Callable[['Candidate'], None]):
        State.add_candidate_listener(callback)

    @staticmethod
    def add_leader_listener(callback: Callable[['Leader'], None]):
        State.add_leader_listener(callback)
