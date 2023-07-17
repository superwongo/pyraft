#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: network
@time: 2023/2/23
"""

import asyncio
from typing import Union, Optional, Callable, Dict, Tuple, ByteString

from pyraft.crypto import AbstractCryptor
from pyraft.config import settings
from pyraft.serializer import AbstractSerializer
from pyraft.log import logger


class UDPProtocol(asyncio.DatagramProtocol):
    def __init__(
            self,
            queue: asyncio.Queue,
            request_handler: Callable[[ByteString, Tuple[str, int]], None],
            serializer: Optional[AbstractSerializer] = None,
            loop: Optional[asyncio.AbstractEventLoop] = None,
            cryptor_enabled: Optional[bool] = False,
            cryptor: Optional[AbstractCryptor] = None
    ):
        self.queue = queue
        self.request_handler = request_handler
        self.serializer = serializer or settings.SERIALIZER
        self.loop = loop or asyncio.get_event_loop()
        self.cryptor_enabled = cryptor_enabled or settings.CRYPTOR_ENABLED
        self.cryptor = cryptor or settings.CRYPTOR
        self.transport: Optional[asyncio.DatagramTransport] = None

    def __call__(self):
        return self

    async def start(self):
        while not self.transport.is_closing():
            data, dest = await self.queue.get()
            if self.serializer:
                data = self.serializer.pack(data)
            if self.cryptor_enabled and self.cryptor:
                data = self.cryptor.encrypt(data)
            self.transport.sendto(data, dest)

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport
        asyncio.ensure_future(self.start(), loop=self.loop)

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        if self.cryptor_enabled and self.cryptor:
            data = self.cryptor.decrypt(data.decode('utf-8'))
        if self.serializer:
            data = self.serializer.unpack(data)
        self.request_handler(data, addr)

    def error_received(self, exc: Exception) -> None:
        logger.error('Error received {}'.format(exc))

    def connection_lost(self, exc: Union[Exception, None]) -> None:
        logger.error('Connection lost {}'.format(exc))
