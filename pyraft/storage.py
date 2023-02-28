#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: storage
@time: 2023/2/17
"""

import asyncio
from abc import ABCMeta, abstractmethod
from typing import Optional, Union, Dict, AnyStr, Any, List, Iterator, Callable
from pathlib import Path
from collections import defaultdict

from pyraft.serializer import AbstractSerializer, JsonSerializer
from pyraft.config import settings
from pyraft.schema import LogEntry

__all__ = ('AbstractDictStorage', 'AbstractListStorage', 'FilePersistentState', 'FilePersistentLog', 'StateMachine')


class AbstractDictStorage(metaclass=ABCMeta):
    def __init__(self, serializer: Optional[AbstractSerializer] = None):
        self.serializer = serializer or JsonSerializer()
        self._cache = {}

    @abstractmethod
    def _get_storage_content(self) -> Union[Dict, AnyStr]:
        return self._cache

    @abstractmethod
    def _set_storage_content(self, content: Union[Dict, AnyStr]) -> None:
        ...

    def get(self, key: str) -> Any:
        if key not in self._cache:
            self.refresh()
        if key not in self._cache:
            raise KeyError(f'{self.__class__.__name__}中不存在键[{key}]')
        return self._cache[key]

    def set(self, key: str, value: Any) -> None:
        self.refresh()
        self._cache[key] = value
        self._set_storage_content(self.serializer.pack(self._cache))

    def update(self, kwargs):
        self.refresh()
        for key, value in kwargs.items():
            self._cache[key] = value
        self._set_storage_content(self.serializer.pack(self._cache))

    def refresh(self):
        content = self._get_storage_content()
        if self.serializer and content:
            content = self.serializer.unpack(content)
        self._cache = content if isinstance(content, Dict) else {}

    def exists(self, key: str) -> bool:
        try:
            self.get(key)
            return True
        except KeyError:
            return False


class AbstractListStorage(metaclass=ABCMeta):
    UPDATE_CACHE_INTERVAL = 5

    def __init__(self, serializer: Optional[AbstractSerializer] = None):
        self.serializer = serializer
        self._cache = []

    @abstractmethod
    def _get_storage_items(self) -> Union[List, AnyStr]:
        return self._cache

    @abstractmethod
    def _set_storage_item(self, item: AnyStr) -> None:
        ...

    @abstractmethod
    def _set_storage_items(self, items: Iterator[Any]) -> None:
        ...

    def __getitem__(self, index: int) -> Any:
        if index > len(self._cache):
            self.refresh()
        if index > len(self._cache):
            raise IndexError(f'{self.__class__.__name__}中不存在索引为[{index}]的项')
        return self._cache[index-1]

    def __len__(self) -> int:
        return len(self._cache)

    def __bool__(self) -> bool:
        return bool(self._cache)

    def refresh(self):
        items = self._get_storage_items()
        if self.serializer:
            items = list(map(self.serializer.unpack, items))
        self._cache = items

    def exists(self, index: int) -> bool:
        try:
            self.__getitem__(index)
            return True
        except IndexError:
            return False

    def append_item(self, item: Any) -> None:
        self._set_storage_item(self.serializer.pack(item))
        if len(self) % self.UPDATE_CACHE_INTERVAL:
            self._cache.append(item)
        else:
            self.refresh()

    def get_items(
            self,
            start_index: Optional[int] = 1,
            end_index: Optional[int] = None
    ) -> List[Any]:
        if start_index > len(self._cache):
            self.refresh()
        if start_index > len(self._cache):
            raise ValueError(f'查询起始索引[{start_index}]越界')
        if end_index:
            if end_index < start_index:
                raise ValueError(f'查询截止索引[{end_index}]应大于等于起始索引[{start_index}]')
            return self._cache[start_index-1:end_index]
        return self._cache[start_index-1:]

    def append_items(self, items: List[Any]) -> None:
        self._set_storage_items(list(map(self.serializer.pack, items)))
        if len(self) % self.UPDATE_CACHE_INTERVAL:
            self._cache.extend(items)
        else:
            self.refresh()

    def erase_from(self, index: int):
        new_cache = self._cache[:index-1]
        self._set_storage_items(map(self.serializer.pack, new_cache))
        self._cache = new_cache


class FileDictStorage(AbstractDictStorage):
    def __init__(
            self,
            filename: str,
            cache_dir: Optional[Union[str, Path]] = None,
            serializer: Optional[AbstractSerializer] = None,
            loop: Optional[asyncio.AbstractEventLoop] = None
    ):
        self.cache_dir = cache_dir or settings.LOG_PATH
        self.cache_dir = Path(self.cache_dir) if isinstance(self.cache_dir, str) else self.cache_dir
        self.file_path = self.cache_dir / filename
        super().__init__(serializer)
        self.serializer = self.serializer or settings.SERIALIZER
        self.loop = loop or asyncio.get_event_loop()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.file_path.touch(exist_ok=True)

    def _get_storage_content(self) -> bytes:
        return self.file_path.read_bytes()

    def _set_storage_content(self, content: bytes) -> None:
        self.file_path.write_bytes(content)


class FileListStorage(AbstractListStorage):
    def __init__(
            self,
            filename: str,
            cache_dir: Optional[Union[str, Path]] = None,
            serializer: Optional[AbstractSerializer] = None,
            loop: Optional[asyncio.AbstractEventLoop] = None
    ):
        self.cache_dir = cache_dir or settings.LOG_PATH
        self.cache_dir = Path(self.cache_dir) if isinstance(self.cache_dir, str) else self.cache_dir
        self.file_path = self.cache_dir / filename
        super().__init__(serializer)
        self.serializer = self.serializer or settings.SERIALIZER
        self.loop = loop or asyncio.get_event_loop()
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.file_path.touch(exist_ok=True)
        self.refresh()

    def _get_storage_items(self) -> List[bytes]:
        with self.file_path.open('rb') as f:
            return f.readlines()

    def _set_storage_item(self, item: bytes) -> None:
        with self.file_path.open('ab') as f:
            f.writelines([item])

    def _set_storage_items(self, items: Iterator[bytes]) -> None:
        with self.file_path.open('ab') as f:
            f.writelines(items)


class FilePersistentState(FileDictStorage):
    def __init__(
            self,
            server_id: str,
            cache_dir: Optional[Union[str, Path]] = None,
            serializer: Optional[AbstractSerializer] = None,
            loop: Optional[asyncio.AbstractEventLoop] = None
    ):
        super().__init__(filename=f'{server_id.replace(":", "-")}.state', cache_dir=cache_dir, serializer=serializer, loop=loop)

    @property
    def current_term(self) -> int:
        return self.get('current_term')

    @current_term.setter
    def current_term(self, value: int):
        self.set('current_term', value)

    @property
    def voted_for(self) -> Union[str, int, None]:
        return self.get('voted_for')

    @voted_for.setter
    def voted_for(self, value: Union[str, int]):
        self.set('voted_for', value)


class FilePersistentLog(FileListStorage):
    def __init__(
            self,
            server_id: str,
            cache_dir: Optional[Union[str, Path]] = None,
            serializer: Optional[AbstractSerializer] = None,
            loop: Optional[asyncio.AbstractEventLoop] = None
    ):
        super().__init__(filename=f'{server_id.replace(":", "-")}.log', cache_dir=cache_dir, serializer=serializer, loop=loop)

        self.commit_index = 0
        self.last_applied = 0
        self.next_index = defaultdict(int)
        self.match_index = defaultdict(int)

    def append_entry(self, entry: LogEntry) -> None:
        self.append_item(entry.to_dict())

    def append_entries(self, entries: List[LogEntry]):
        self.append_items([entry.to_dict() for entry in entries])

    @property
    def last_log_index(self):
        return len(self._cache)

    @property
    def last_log_term(self):
        return self._cache[-1]['term'] if self._cache else 0


class StateMachine(AbstractDictStorage):
    def __init__(self, apply_handler: Optional[Callable[['StateMachine', Dict[str, Any]], None]] = None):
        super().__init__()
        self.apply_handler = apply_handler

    def _get_storage_content(self) -> Dict:
        return super()._get_storage_content()

    def _set_storage_content(self, content: Dict) -> None:
        super()._set_storage_content(content)

    def apply(self, command: Dict[str, Any]) -> None:
        if callable(self.apply_handler):
            if asyncio.iscoroutinefunction(self.apply_handler):
                asyncio.ensure_future(self.apply_handler(self, command))
            else:
                self.apply_handler(self, command)
        else:
            self.update(command)
