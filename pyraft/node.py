#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: node
@time: 2023/2/17
"""

import asyncio
from typing import Optional, Union, Dict, Tuple, Any, Set, Iterator

from pyraft.state import State
from pyraft.network import UDPProtocol


class Node:
    def __init__(self, host: str, port: int, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.host = host
        self.port = port
        self.loop = loop or asyncio.get_event_loop()
        self.cluster: Set[Iterator[str, int]] = set()
        self.state = State(self)
        self.requests = asyncio.Queue(loop=self.loop)
        self.transport = None

    async def start(self):
        protocol = UDPProtocol(self.requests, self.request_handler, loop=self.loop)
        self.transport, _ = await self.loop.create_datagram_endpoint(protocol, local_addr=(self.host, self.port))
        self.state.start()

    def stop(self):
        self.state.stop()
        self.transport.close()

    def update_cluster(self, host: str, port: int):
        self.cluster.update((host, port))

    def request_handler(self, data: Dict, sender: Tuple[str, int]) -> None:
        self.state.request_handler(data, sender)

    async def send(self, data: Any, dest: Union[str, Tuple[str, int]]):
        if isinstance(dest, str):
            host, port = dest.split(':')
            dest = (host, int(port))
        await self.requests.put((data, dest))

    async def broadcast(self, data: Any):
        tasks = [self.send(data, dest) for dest in self.cluster]
        await asyncio.gather(*tasks, loop=self.loop)
