#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: config
@time: 2023/2/23
"""

from pathlib import Path
from typing import Union, Optional
from dataclasses import dataclass

from pyraft.crypto import AbstractCryptor, AESCryptor
from pyraft.serializer import AbstractSerializer, MsgPackSerializer


__all__ = ('settings',)


@dataclass
class Settings:
    LOG_PATH: Union[str, Path] = Path('~/.raft').expanduser()
    SERIALIZER: Optional[AbstractSerializer] = MsgPackSerializer()

    HEARTBEAT_INTERVAL: float = 3 * 0.1
    STEP_DOWN_MISSED_HEARTBEATS: int = 5
    ELECTION_INTERVAL_SPREAD: int = 3
    STEP_DOWN_INTERVAL: Optional[float] = None
    ELECTION_INTERVAL: Optional[float] = None

    APPEND_ENTRIES_MAX_NUM: Optional[int] = 3

    CRYPTOR_ENABLED: bool = False
    CRYPTOR_SECRET: bytes = b'raftos sample secret key'
    CRYPTOR: Optional[AbstractCryptor] = None


settings = Settings()
settings.CRYPTOR = AESCryptor(settings.CRYPTOR_SECRET) if settings.CRYPTOR_ENABLED else None
settings.STEP_DOWN_INTERVAL = settings.HEARTBEAT_INTERVAL * settings.STEP_DOWN_MISSED_HEARTBEATS
settings.ELECTION_INTERVAL = (
    settings.STEP_DOWN_INTERVAL,
    settings.STEP_DOWN_INTERVAL * settings.ELECTION_INTERVAL_SPREAD
)
