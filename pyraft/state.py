#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: state
@time: 2023/2/17
"""

import random
import asyncio
import functools
from abc import ABCMeta, abstractmethod
from typing import Union, Dict, Type, Callable, Tuple, Optional, List, Any, ByteString
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

from pyraft.storage import StateStorage, LogsStorage, StateMachine
from pyraft.timer import Timer
from pyraft.config import settings
from pyraft.log import logger
from pyraft.schema import rpc_request_mapping, LogEntry, RequestVote, RequestVoteResponse, AppendEntries, \
    AppendEntriesResponse


THREAD_POOL_EXECUTOR = ThreadPoolExecutor()


def validate_term(func):
    @functools.wraps(func)
    def wrapped(
            role: 'BaseRole',
            data: Union[RequestVote, RequestVoteResponse, AppendEntries, AppendEntriesResponse],
            sender: Tuple[str, int]
    ):
        if data.term > role.storage.current_term:
            role.storage.current_term = data.term
            if not isinstance(role, Follower):
                # TODO 需要测试变更为follower后，是否会执行新的follower的func
                role.state.to_follower()
        elif data.term < role.storage.current_term:
            if isinstance(data, RequestVote):
                response = RequestVoteResponse(term=role.storage.current_term, vote_granted=False)
                role.state.send(response.to_dict(), sender)
                return
            elif isinstance(data, AppendEntries):
                response = AppendEntriesResponse(
                    term=role.storage.current_term,
                    success=False,
                    last_log_index=role.log.last_log_index,
                    last_log_term=role.log.last_log_term,
                    request_id=data.request_id
                )
                role.state.send(response.to_dict(), sender)
                return
        return func(role, data, sender)
    return wrapped


def validate_commit_index(func):
    @functools.wraps(func)
    def wrapped(
            role: 'BaseRole',
            data: Union[RequestVote, RequestVoteResponse, AppendEntries, AppendEntriesResponse],
            sender: Tuple[str, int]
    ):
        result = func(role, data, sender)
        if role.log.commit_index > role.log.last_applied:
            for not_applied in range(role.log.last_applied + 1, role.log.commit_index + 1):
                command = role.log[not_applied]['command']
                role.state_machine.apply(command)
                logger.debug(f'日志序列[{not_applied}]中command[{command}]已应用到状态机中')
                role.log.last_applied += 1
                if isinstance(role, Leader) and not_applied in role.apply_future_dict:
                    logger.info(f'日志序列[{not_applied}]已提交')
                    try:
                        apply_future: asyncio.Future = role.apply_future_dict.pop(not_applied)
                        if not apply_future.done():
                            apply_future.set_result(not_applied)
                    except (asyncio.futures.InvalidStateError, AttributeError):
                        pass
        return result
    return wrapped


def leader_required(func):
    @functools.wraps(func)
    async def wrapped(cls: 'State', *args, **kwargs):
        await cls.wait_for_election_success()
        if not isinstance(cls.leader, Leader):
            raise RuntimeError(f'选举出的leader[{cls.leader}]异常')
        return await func(cls, *args, **kwargs)
    return wrapped


class State:
    loop: Optional[asyncio.AbstractEventLoop] = None

    leader: Optional[Union['Leader', str]] = None
    leader_future: Optional[asyncio.Future] = None

    wait_until_leader_id: Optional[str] = None
    wait_until_leader_future: Optional[asyncio.Future] = None

    on_follower_callback: Optional[Callable[['Follower'], None]] = None
    on_candidate_callback: Optional[Callable[['Candidate'], None]] = None
    on_leader_callback: Optional[Callable[['Leader'], None]] = None

    def __init__(self, server):
        self.server = server
        self.__class__.loop = self.server.loop

        self.storage = StateStorage(self.id)
        self.log = LogsStorage(self.id)
        self.state_machine = StateMachine()

        self.role = Follower(self)

    def start(self):
        self.role.start()

    def stop(self):
        self.role.stop()

    @staticmethod
    def get_server_id(host: str, port: int) -> str:
        return f'{host}:{port}'

    @property
    def id(self) -> str:
        return self.get_server_id(*self.server.addr)

    def _change_role(self, new_role: Type['BaseRole']):
        self.role.stop()
        self.role = new_role(self)
        self.role.start()

    @property
    def leader_id(self):
        cls = self.__class__
        return cls.leader.id if isinstance(cls.leader, Leader) else cls.leader

    def set_leader(self, leader):
        cls = self.__class__
        cls.leader = leader
        if cls.leader and cls.leader_future and not cls.leader_future.done():
            cls.leader_future.set_result(cls.leader)

        if cls.wait_until_leader_id and (
            cls.wait_until_leader_future and not cls.wait_until_leader_future.done()
        ) and self.leader_id == cls.wait_until_leader_id:
            cls.wait_until_leader_future.set_result(cls.leader)

    @classmethod
    def add_follower_listener(cls, callback: Callable[['Follower'], None]):
        cls.on_follower_callback = callback

    @classmethod
    def add_candidate_listener(cls, callback: Callable[['Candidate'], None]):
        cls.on_candidate_callback = callback

    @classmethod
    def add_leader_listener(cls, callback: Callable[['Leader'], None]):
        cls.on_leader_callback = callback

    def to_follower(self):
        self._change_role(Follower)
        self.set_leader(None)
        if callable(self.__class__.on_follower_callback):
            if asyncio.iscoroutinefunction(self.__class__.on_follower_callback):
                asyncio.ensure_future(self.__class__.on_follower_callback(self.role), loop=self.loop)
            else:
                self.__class__.on_follower_callback(self.role)

    def to_candidate(self):
        self._change_role(Candidate)
        self.set_leader(None)
        if callable(self.__class__.on_candidate_callback):
            if asyncio.iscoroutinefunction(self.__class__.on_candidate_callback):
                asyncio.ensure_future(self.__class__.on_candidate_callback(self.role), loop=self.loop)
            else:
                self.__class__.on_candidate_callback(self.role)

    def to_leader(self):
        self._change_role(Leader)
        self.set_leader(self.role)
        if callable(self.__class__.on_leader_callback):
            if asyncio.iscoroutinefunction(self.__class__.on_leader_callback):
                asyncio.ensure_future(self.__class__.on_leader_callback(self.role), loop=self.loop)
            else:
                self.__class__.on_leader_callback(self.role)

    def send(self, data: Dict, dest: Union[str, Tuple[str, int]]):
        data = self.log.serializer.pack(data)
        self.server.send(data, dest)

    def broadcast(self, data: Dict):
        data = self.log.serializer.pack(data)
        self.server.broadcast(data)

    def request_handler(self, data: ByteString, sender: Tuple[str, int]):
        data = self.log.serializer.unpack(data)
        request_type = data.get('type')
        if not request_type or not hasattr(rpc_request_mapping, request_type):
            return
        dc = getattr(rpc_request_mapping, request_type)
        handler = getattr(self.role, f'on_receive_{request_type}', None)
        if handler:
            handler(dc(**data), sender)

    def is_majority(self, count: int) -> bool:
        return count > (len(self.server.cluster) // 2)

    @property
    def cluster(self) -> List[str]:
        return [self.get_server_id(*follower) for follower in self.server.cluster]

    @classmethod
    def get_leader(cls):
        return cls.leader.id if isinstance(cls.leader, Leader) else cls.leader

    @classmethod
    async def wait_for_election_success(cls):
        if cls.leader is None:
            cls.leader_future = asyncio.Future(loop=cls.loop)
            await cls.leader_future
            cls.leader_future = None

    @classmethod
    async def wait_until_leader(cls, server_id: str):
        if not server_id:
            raise ValueError('节点ID不可为空')
        if cls.get_leader() != server_id:
            cls.wait_until_leader_id = server_id
            cls.wait_until_leader_future = asyncio.Future(loop=cls.loop)
            await cls.wait_until_leader_future
            cls.wait_until_leader_id = None
            cls.wait_until_leader_future = None

    @classmethod
    @leader_required
    async def get_value(cls, name: str) -> Any:
        return cls.leader.state_machine[name]

    @classmethod
    @leader_required
    async def set_value(cls, name: str, value: Any):
        await cls.leader.execute_command({name: value})
        logger.debug(f'参数{name}已设置为{value}')


class BaseRole(metaclass=ABCMeta):
    def __init__(self, state: State):
        self.state = state

        self.storage = self.state.storage
        self.log = self.state.log
        self.state_machine = self.state.state_machine
        self.id = self.state.id
        self.loop = self.state.loop

    @abstractmethod
    def start(self):
        ...

    @abstractmethod
    def stop(self):
        ...

    @validate_term
    def on_receive_request_vote(
            self,
            data: Union[RequestVote, RequestVoteResponse, AppendEntries, AppendEntriesResponse],
            sender: Tuple[str, int]
    ):
        ...

    @validate_term
    def on_receive_request_vote_response(
            self,
            data: Union[RequestVote, RequestVoteResponse, AppendEntries, AppendEntriesResponse],
            sender: Tuple[str, int]
    ):
        ...

    @validate_commit_index
    @validate_term
    def on_receive_append_entries(
            self,
            data: Union[RequestVote, RequestVoteResponse, AppendEntries, AppendEntriesResponse],
            sender: Tuple[str, int]
    ):
        ...

    @validate_commit_index
    @validate_term
    def on_receive_append_entries_response(
            self,
            data: Union[RequestVote, RequestVoteResponse, AppendEntries, AppendEntriesResponse],
            sender: Tuple[str, int]
    ):
        ...


class Follower(BaseRole):
    def __init__(self, state: State):
        super().__init__(state)
        self.heartbeat_timer = Timer(self.election_interval(), self.start_election, loop=self.loop)

    def start(self):
        self.init_storage()
        self.heartbeat_timer.start()

    def stop(self):
        self.heartbeat_timer.stop()

    def init_storage(self):
        if not self.storage.exists('current_term'):
            self.storage.current_term = 0
        self.storage.voted_for = None

    @staticmethod
    def election_interval():
        return random.uniform(*settings.ELECTION_INTERVAL)

    def start_election(self):
        self.state.to_candidate()

    @validate_commit_index
    @validate_term
    def on_receive_append_entries(self, data: AppendEntries, sender: Tuple[str, int]):
        self.state.set_leader(data.leader_id)

        if self.log.last_log_index < data.prev_log_index \
                or (data.prev_log_index and self.log[data.prev_log_index]['term'] != data.prev_log_term):
            response = AppendEntriesResponse(
                term=self.storage.current_term,
                success=False,
                last_log_index=self.log.last_log_index,
                last_log_term=self.log.last_log_term,
                request_id=data.request_id
            )
            self.state.send(response.to_dict(), sender)
            return

        if self.log.last_log_index > data.prev_log_index:
            self.log.erase_from(data.prev_log_index)

        self.log.append_entries(data.entries)
        if data.leader_commit > self.log.commit_index:
            self.log.commit_index = min(data.leader_commit, self.log.last_log_index)

        response = AppendEntriesResponse(
            term=self.storage.current_term,
            success=True,
            last_log_index=self.log.last_log_index,
            last_log_term=self.log.last_log_term,
            request_id=data.request_id
        )
        self.state.send(response.to_dict(), sender)
        self.heartbeat_timer.reset()

    @validate_term
    def on_receive_request_vote(self, data: RequestVote, sender: Tuple[str, int]):
        vote_granted = False
        if self.storage.voted_for is None or self.storage.voted_for == data.candidate_id:
            vote_granted = True
        if data.last_log_term > self.log.last_log_term \
                or (data.last_log_term == self.log.last_log_term and data.last_log_index >= self.log.last_log_index):
            vote_granted = True
        if vote_granted:
            self.storage.voted_for = data.candidate_id
        response = RequestVoteResponse(term=self.storage.current_term, vote_granted=vote_granted)
        self.state.send(response.to_dict(), sender)


class Candidate(BaseRole):
    def __init__(self, state: State):
        super().__init__(state)
        self.election_timer = Timer(self.election_interval(), self.state.to_follower, loop=self.loop)
        self.vote_count = 0

    def start(self):
        self.init_storage()
        self.vote_count = 1
        self.rpc_request_vote()
        self.election_timer.start()

    def stop(self):
        self.election_timer.stop()

    def init_storage(self):
        self.storage.current_term += 1
        self.storage.voted_for = self.id

    @staticmethod
    def election_interval():
        return random.uniform(*settings.ELECTION_INTERVAL)

    def rpc_request_vote(self):
        request = RequestVote(
            term=self.storage.current_term,
            candidate_id=self.id,
            last_log_index=self.log.last_log_index,
            last_log_term=self.log.last_log_term
        )
        self.state.broadcast(request.to_dict())

    @validate_term
    def on_receive_request_vote_response(self, data: RequestVoteResponse, sender: Tuple[str, int]):
        if data.vote_granted:
            self.vote_count += 1
            if self.state.is_majority(self.vote_count):
                self.state.to_leader()

    @validate_term
    def on_receive_append_entries(self, data: AppendEntries, sender: Tuple[str, int]):
        if data.term == self.storage.current_term:
            self.state.to_follower()


class Leader(BaseRole):
    def __init__(self, state: State):
        super().__init__(state)
        self.heartbeat_timer = Timer(settings.HEARTBEAT_INTERVAL, self.heartbeat)
        self.step_down_timer = Timer(settings.STEP_DOWN_INTERVAL, self.state.to_follower)
        self.request_id = 0
        self.response_mapping = defaultdict(set)
        # TODO 需要增加异常超时自动清除机制
        self.apply_future_dict = {}

    def start(self):
        self.init_log()
        self.heartbeat()
        self.heartbeat_timer.start()
        self.step_down_timer.start()

    def stop(self):
        self.heartbeat_timer.stop()
        self.step_down_timer.stop()

    def init_log(self):
        self.log.next_index = {follower: self.log.last_log_index + 1 for follower in self.state.cluster}
        self.log.match_index = {follower: 0 for follower in self.state.cluster}

    def heartbeat(self):
        self.request_id += 1
        asyncio.ensure_future(self.loop.run_in_executor(THREAD_POOL_EXECUTOR, self.rpc_append_entries), loop=self.loop)

    def rpc_append_entries(self, server_id: Optional[str] = None):
        server_id_list = [server_id] if server_id else self.state.cluster
        for server_id in server_id_list:
            next_index = self.log.next_index[server_id]
            prev_index = next_index - 1
            entries = self.log.get_items(next_index, next_index + settings.APPEND_ENTRIES_MAX_NUM) \
                if self.log.last_log_index >= next_index else []
            request = AppendEntries(
                term=self.storage.current_term,
                leader_id=self.id,
                prev_log_index=prev_index,
                prev_log_term=self.log[prev_index]['term'] if self.log.exists(prev_index) else 0,
                entries=entries,
                leader_commit=self.log.commit_index,
                request_id=self.request_id
            )
            self.state.send(request.to_dict(), server_id)

    @validate_commit_index
    @validate_term
    def on_receive_append_entries_response(self, data: AppendEntriesResponse, sender: Tuple[str, int]):
        sender_id = self.state.get_server_id(*sender)
        self.response_mapping[data.request_id].add(sender_id)
        if self.state.is_majority(len(self.response_mapping) + 1):
            self.step_down_timer.reset()
            del self.response_mapping[data.request_id]

        if data.success:
            if data.last_log_index > self.log.match_index[sender_id]:
                self.log.next_index[sender_id] = self.log.last_log_index + 1
                self.log.match_index[sender_id] = self.log.last_log_index
            self.update_commit_index()
        else:
            next_index = self.log.next_index[sender_id]
            prev_index = next_index - 1
            if prev_index > data.last_log_index:
                self.log.next_index[sender_id] = data.last_log_index + 1
            else:
                self.log.next_index[sender_id] = max(next_index - 1, 1)

        if self.log.last_log_index >= self.log.next_index[sender_id]:
            self.rpc_append_entries(sender_id)

    def update_commit_index(self):
        committed_on_majority = 0
        for index in range(self.log.commit_index + 1, self.log.last_log_index + 1):
            committed_count = len([filter(lambda x: x >= index, self.log.match_index.values())])
            if self.state.is_majority(committed_count + 1) and self.log[index]['term'] == self.storage.current_term:
                committed_on_majority = index
            else:
                continue
        if committed_on_majority > self.log.commit_index:
            self.log.commit_index = committed_on_majority

    async def execute_command(self, command):
        apply_future = asyncio.Future(loop=self.loop)
        await self.loop.run_in_executor(
            THREAD_POOL_EXECUTOR, self.log.append_entry, LogEntry(term=self.storage.current_term, command=command)
        )
        self.heartbeat_timer.reset()
        self.apply_future_dict[self.log.last_log_index] = apply_future
        await self.loop.run_in_executor(THREAD_POOL_EXECUTOR, self.rpc_append_entries)
        await apply_future
        logger.debug(f'命令[{command}]已同步发送至其他节点')
