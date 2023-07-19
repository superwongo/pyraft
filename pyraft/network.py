#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: network
@time: 2023/2/23
"""

import asyncio
from typing import Union, Optional, Callable, Tuple

from pyraft.crypto import AbstractCryptor
from pyraft.config import settings
from pyraft.log import logger


class UDPProtocol(asyncio.DatagramProtocol):
    def __init__(
            self,
            request_handler: Callable[[bytes, Tuple[str, int]], None],
            on_con_lost: asyncio.Future,
            cryptor_enabled: Optional[bool] = False,
            cryptor: Optional[AbstractCryptor] = None,
            loop: Optional[asyncio.AbstractEventLoop] = None
    ):
        self.request_handler = request_handler
        self.on_con_lost = on_con_lost
        self.cryptor_enabled = cryptor_enabled or settings.CRYPTOR_ENABLED
        self.cryptor = cryptor or settings.CRYPTOR
        self.loop = loop or asyncio.get_event_loop()
        self.transport: Optional[asyncio.DatagramTransport] = None

    def __call__(self):
        return self

    def send(self, data: bytes, addr: Tuple[str, int]):
        if self.cryptor_enabled and self.cryptor:
            data = self.cryptor.encrypt(data)
        self.transport.sendto(data, addr)

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        if self.cryptor_enabled and self.cryptor:
            data = self.cryptor.decrypt(data)
        self.request_handler(data, addr)

    def error_received(self, exc: Exception) -> None:
        logger.error('Error received {}'.format(exc))
        self.transport.abort()

    def connection_lost(self, exc: Union[Exception, None]) -> None:
        logger.error('Connection closed')
        self.on_con_lost.set_result(True)
