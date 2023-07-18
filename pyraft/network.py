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
            loop: Optional[asyncio.AbstractEventLoop] = None,
            cryptor_enabled: Optional[bool] = False,
            cryptor: Optional[AbstractCryptor] = None
    ):
        self.request_handler = request_handler
        self.loop = loop or asyncio.get_event_loop()
        self.cryptor_enabled = cryptor_enabled or settings.CRYPTOR_ENABLED
        self.cryptor = cryptor or settings.CRYPTOR
        self.transport: Optional[asyncio.DatagramTransport] = None
        self.sender_queue = asyncio.Queue()
        self.on_error_received: Optional[asyncio.Future] = None
        self.is_error_received = False

    def __call__(self):
        return self

    async def start(self, host: str, port: int, is_daemon: bool = True) -> None:
        await self.create_endpoint(host, port, is_daemon)

    def stop(self):
        self.transport.close()

    async def transport_listener(self, host: str, port: int) -> None:
        while not self.transport.is_closing():
            self.on_error_received = self.loop.create_future()
            try:
                await self.on_error_received
            finally:
                logger.debug(f'Transport: {host}:{port}准备重启')
                await self.create_endpoint(host, port)

    async def create_endpoint(self, host: str, port: int, is_daemon: bool = True) -> None:
        self.transport, _ = await self.loop.create_datagram_endpoint(self, local_addr=(host, port))
        if is_daemon:
            asyncio.ensure_future(self.transport_listener(host, port), loop=self.loop)
        else:
            await self.transport_listener(host, port)

    def send(self, data: bytes, addr: Tuple[str, int]):
        asyncio.ensure_future(self.sender_queue.put((data, addr)), loop=self.loop)

    async def sender_listener(self):
        if self.transport is None:
            raise RuntimeError('未启动UDP端口监听，是否未执行start()方法')
        while not self.transport.is_closing():
            data, addr = await self.sender_queue.get()
            if self.cryptor_enabled and self.cryptor:
                data = self.cryptor.encrypt(data)
            self.transport.sendto(data, addr)

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        self.transport = transport
        asyncio.ensure_future(self.sender_listener(), loop=self.loop)

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        if self.cryptor_enabled and self.cryptor:
            data = self.cryptor.decrypt(data)
        self.request_handler(data, addr)

    def error_received(self, exc: Exception) -> None:
        logger.error('Error received {}'.format(exc))
        self.is_error_received = True
        self.transport.abort()

    def connection_lost(self, exc: Union[Exception, None]) -> None:
        if exc:
            logger.error('Connection lost {}'.format(exc))
        if self.is_error_received and self.on_error_received:
            self.is_error_received = False
            self.on_error_received.set_result(True)
