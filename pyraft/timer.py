#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: timer
@time: 2023/2/24
"""

import asyncio
from typing import Optional, Callable


class Timer:
    def __init__(self, interval: float, callback: Callable, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.interval = interval
        self.callback = callback
        self.loop = loop or asyncio.get_event_loop()
        self.is_active = False
        self._handler = None

    def start(self):
        self.is_active = True
        self._handler = self.loop.call_later(self.interval, self._run)

    def _run(self):
        if self.is_active:
            self.callback()
            self._handler = self.loop.call_later(self.interval, self._run)

    def stop(self):
        self.is_active = False
        if not self._handler:
            raise RuntimeError('The timer is not started')
        self._handler.cancel()

    def reset(self):
        self.stop()
        self.start()
