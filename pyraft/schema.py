#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: schema
@time: 2023/2/28
"""

from typing import Union, Optional, List, Dict, Any
from dataclasses import dataclass, asdict
from collections import namedtuple


__all__ = (
    'rpc_request_mapping', 'LogEntry', 'RequestVote', 'RequestVoteResponse', 'AppendEntries', 'AppendEntriesResponse'
)


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

rpc_request_mapping = RPCRequestMapping(RequestVote, RequestVoteResponse, AppendEntries, AppendEntriesResponse)
