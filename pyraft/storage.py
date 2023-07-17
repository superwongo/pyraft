#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: storage
@time: 2023/2/17
"""

import asyncio
import sqlite3
import traceback
from abc import ABCMeta, abstractmethod
from typing import Optional, Union, Dict, AnyStr, Any, List, Callable
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from contextlib import contextmanager

from pyraft.serializer import AbstractSerializer, JsonSerializer
from pyraft.config import settings
from pyraft.schema import LogEntry


__all__ = ('AbstractDictStorage', 'AbstractListStorage', 'StateStorage', 'LogsStorage', 'StateMachine')


cache_dir = Path(settings.LOG_PATH) if isinstance(settings.LOG_PATH, str) else settings.LOG_PATH
cache_dir.mkdir(parents=True, exist_ok=True)
DB_URI = (cache_dir / 'pyraft.db').absolute()


@contextmanager
def sqlite(cursor: Optional[sqlite3.Cursor] = None) -> sqlite3.Cursor:
    if cursor:
        yield cursor
        return

    con = sqlite3.connect(DB_URI)
    cursor = con.cursor()
    try:
        yield cursor
        con.commit()
    except Exception as e:
        traceback.print_exception(e)
        con.rollback()
    finally:
        cursor.close()
        con.close()


class AbstractDictStorage(metaclass=ABCMeta):
    @abstractmethod
    def get(self, key: str) -> Any:
        ...

    @abstractmethod
    def set(self, key: str, value: Any) -> None:
        ...

    @abstractmethod
    def update(self, kwargs):
        ...

    def __getitem__(self, key: str) -> Any:
        return self.get(key)

    def exists(self, key: str) -> bool:
        try:
            self.get(key)
            return True
        except KeyError:
            return False


class AbstractListStorage(metaclass=ABCMeta):
    @abstractmethod
    def get_item(self, index: int) -> Any:
        ...

    def exists(self, index: int) -> bool:
        try:
            self.get_item(index)
            return True
        except IndexError:
            return False

    @abstractmethod
    def append_item(self, item: Any) -> None:
        ...

    @abstractmethod
    def get_items(
            self,
            start_index: Optional[int] = 1,
            end_index: Optional[int] = None
    ) -> List[Any]:
        ...

    @abstractmethod
    def append_items(self, items: List[Any]) -> None:
        ...

    @abstractmethod
    def erase_from(self, index: int):
        ...

    def __getitem__(self, index: int) -> Any:
        return self.get_item(index)


class StateStorage(AbstractDictStorage):
    def __init__(self, server_id: str):
        self.table_name = f'state_{server_id.replace(".", "_").replace(":", "_")}'
        self._init_db()

    def _init_db(self):
        with sqlite() as db:
            db.execute(
                f"""
                    create table if not exists {self.table_name} (
                        key varchar(20) primary key,
                        value varchar(100) not null 
                    )
                """
            )

    def get(self, key: str, cursor: sqlite3.Cursor = None) -> Any:
        with sqlite(cursor) as db:
            res = db.execute(f"select value from {self.table_name} where key='{key}'")
            value = res.fetchone()
        if not value:
            raise KeyError(f'{self.__class__.__name__}中不存在键[{key}]')
        return value[0]

    def set(self, key: str, value: AnyStr, cursor: sqlite3.Cursor = None) -> Optional[str]:
        with sqlite(cursor) as db:
            try:
                v = self.get(key, db)
                if v != value:
                    db.execute(f"update {self.table_name} set value='{value}' where key='{key}'")
                return value
            except KeyError:
                db.execute(f"insert into {self.table_name} (key, value) values ('{key}', '{value}')")
                return value

    def update(self, kwargs: Dict[str, AnyStr], cursor: sqlite3.Cursor = None) -> List[Optional[str]]:
        with sqlite(cursor) as db:
            return [self.set(key, value, db) for key, value in kwargs.items()]

    @property
    def current_term(self) -> int:
        return int(self.get('current_term'))

    @current_term.setter
    def current_term(self, value: int):
        self.set('current_term', str(value))

    @property
    def voted_for(self) -> Union[str, int, None]:
        return self.get('voted_for')

    @voted_for.setter
    def voted_for(self, value: Union[str, int]):
        self.set('voted_for', value)


class LogsStorage(AbstractListStorage):
    def __init__(
            self,
            server_id: str,
            serializer: Optional[AbstractSerializer] = None
    ):
        self.table_name = f'logs_{server_id.replace(".", "_").replace(":", "_")}'
        self.serializer = serializer or settings.SERIALIZER or JsonSerializer()
        self._init_db()

        self.commit_index = 0
        self.last_applied = 0
        self.next_index = defaultdict(int)
        self.match_index = defaultdict(int)

    def _init_db(self):
        with sqlite() as db:
            db.execute(
                f"""
                    create table if not exists {self.table_name} (
                        idx integer primary key,
                        entry varchar(100) not null,
                        datetime timestamp not null 
                    )
                """
            )

    def get_item(self, index: int, cursor: sqlite3.Cursor = None) -> Any:
        with sqlite(cursor) as db:
            sql = f"select entry from {self.table_name} where idx = {index}"
            res = db.execute(sql)
            item = res.fetchone()
        if not item:
            raise IndexError(f'{self.__class__.__name__}中不存在索引为[{index}]的项')
        return self.serializer.unpack(item[0])

    def count(self, cursor: sqlite3.Cursor = None) -> int:
        with sqlite(cursor) as db:
            res = db.execute(f"select count(*) from {self.table_name}")
            return int(res.fetchone()[0])

    def append_item(self, item: Any, cursor: sqlite3.Cursor = None) -> None:
        with sqlite(cursor) as db:
            db.execute(
                f"insert into {self.table_name} (entry, datetime) values (?, ?)",
                (self.serializer.pack(item).decode("utf8"), datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            )

    def get_items(
            self,
            start_index: Optional[int] = 1,
            end_index: Optional[int] = None,
            cursor: sqlite3.Cursor = None
    ) -> List[Any]:
        with sqlite(cursor) as db:
            if start_index > self.count(db):
                raise ValueError(f'查询起始索引[{start_index}]越界')
            sql = f"select entry from {self.table_name} where idx >= {start_index}"
            if end_index:
                if end_index < start_index:
                    raise ValueError(f'查询截止索引[{end_index}]应大于等于起始索引[{start_index}]')
                sql = f'{sql} and idx <= {end_index}'
            res = db.execute(sql)
            return [self.serializer.unpack(item[0]) for item in res.fetchall()]

    def append_items(self, items: List[Any], cursor: sqlite3.Cursor = None) -> None:
        with sqlite(cursor) as db:
            db.executemany(
                f"insert into {self.table_name} (entry, datetime) values (?, ?)",
                [(self.serializer.pack(item).decode("utf8"), datetime.now().strftime("%Y-%m-%d %H:%M:%S")) for item in items]
            )

    def erase_from(self, index: int, cursor: sqlite3.Cursor = None):
        with sqlite(cursor) as db:
            db.execute(f"delete from {self.table_name} where idx > {index}")

    def append_entry(self, entry: LogEntry) -> None:
        self.append_item(entry.to_dict())

    def append_entries(self, entries: List[Union[LogEntry, Dict]]):
        self.append_items([entry.to_dict() if isinstance(entry, LogEntry) else entry for entry in entries])

    @property
    def last_log_index(self):
        return self.count()

    @property
    def last_log_term(self):
        with sqlite() as db:
            last_log_index = self.count(db)
            if last_log_index:
                item = self.get_item(last_log_index, cursor=db)
                return item['term']
            return 0


class StateMachine(AbstractDictStorage):
    def __init__(self, apply_handler: Optional[Callable[['StateMachine', Dict[str, Any]], None]] = None):
        self._cache = {}
        self.apply_handler = apply_handler

    def get(self, key: str) -> Any:
        if key not in self._cache:
            raise KeyError(f'{self.__class__.__name__}中不存在键[{key}]')
        return self._cache[key]

    def set(self, key: str, value: Any) -> None:
        self._cache[key] = value

    def update(self, kwargs):
        for key, value in kwargs.items():
            self.set(key, value)

    def apply(self, command: Dict[str, Any]) -> None:
        if callable(self.apply_handler):
            if asyncio.iscoroutinefunction(self.apply_handler):
                asyncio.ensure_future(self.apply_handler(self, command))
            else:
                self.apply_handler(self, command)
        else:
            self.update(command)
