#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: config
@time: 2023/2/23
"""

from pathlib import Path
from typing import Union, Optional, List, Dict, Any
from dataclasses import dataclass, asdict
from collections import namedtuple

from pyraft.crypto import AbstractCryptor, AESCryptor
from pyraft.serializer import AbstractSerializer, MsgPackSerializer


__all__ = (
    'settings', 'rpc_request_mapping', 'LogEntry', 'RequestVote', 'RequestVoteResponse', 'AppendEntries',
    'AppendEntriesResponse'
)


@dataclass
class Settings:
    LOG_PATH: Union[str, Path] = Path('~/.raft/.cache').expanduser()
    SERIALIZER: Optional[AbstractSerializer] = MsgPackSerializer()

    HEARTBEAT_INTERVAL: float = 0.3
    STEP_DOWN_MISSED_HEARTBEATS: int = 5
    ELECTION_INTERVAL_SPREAD: int = 3
    STEP_DOWN_INTERVAL: Optional[float] = None
    ELECTION_INTERVAL: Optional[float] = None

    APPEND_ENTRIES_MAX_NUM: Optional[int] = 3

    CRYPTOR_ENABLED: bool = False
    CRYPTOR_SECRET: str = 'raftos sample secret key'
    CRYPTOR: Optional[AbstractCryptor] = None


@dataclass
class LogEntry:
    term: int
    command: Dict[str, Any]

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class RequestVote:
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int
    type: Optional[str] = 'request_vote'

    def to_dict(self) -> Dict:
        return asdict(self)


@dataclass
class RequestVoteResponse:
    term: int
    vote_granted: bool
    type: Optional[str] = 'request_vote_response'

    def to_dict(self):
        return asdict(self)


@dataclass
class AppendEntries:
    term: int
    leader_id: Union[str, int]
    prev_log_index: int
    prev_log_term: int
    entries: List[LogEntry]
    leader_commit: int
    request_id: int
    type: Optional[str] = 'append_entries'

    def to_dict(self):
        return asdict(self)


@dataclass
class AppendEntriesResponse:
    term: int
    success: bool
    last_log_index: int
    last_log_term: int
    request_id: int
    type: Optional[str] = 'append_entries_response'

    def to_dict(self) -> Dict:
        return asdict(self)


RPCRequestMapping = namedtuple(
    'RPCRequestMapping', ['request_vote', 'request_vote_response', 'append_entries', 'append_entries_response']
)


settings = Settings()
settings.CRYPTOR = AESCryptor(settings.CRYPTOR_SECRET) if settings.CRYPTOR_ENABLED else None
settings.STEP_DOWN_INTERVAL = settings.HEARTBEAT_INTERVAL * settings.STEP_DOWN_MISSED_HEARTBEATS
settings.ELECTION_INTERVAL = (
    settings.STEP_DOWN_INTERVAL,
    settings.STEP_DOWN_INTERVAL * settings.ELECTION_INTERVAL_SPREAD
)

rpc_request_mapping = RPCRequestMapping(RequestVote, RequestVoteResponse, AppendEntries, AppendEntriesResponse)
