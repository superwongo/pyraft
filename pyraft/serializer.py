#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: serializer
@time: 2023/2/17
"""

import json
from abc import ABCMeta, abstractmethod
from typing import Optional, AnyStr, Dict

import msgpack


class AbstractSerializer(metaclass=ABCMeta):
    def __init__(self, encoding: Optional[str] = 'utf-8'):
        self.encoding = encoding

    @abstractmethod
    def pack(self, data: Dict) -> AnyStr:
        ...

    @abstractmethod
    def unpack(self, data: AnyStr) -> Dict:
        ...


class JsonSerializer(AbstractSerializer):
    def pack(self, data: Dict) -> str:
        return json.dumps(data)

    def unpack(self, data: str) -> Dict:
        return json.loads(data)


class MsgPackSerializer(AbstractSerializer):
    def pack(self, data: Dict) -> str:
        return msgpack.packb(data, use_bin_type=True)

    def unpack(self, data: str) -> Dict:
        return msgpack.unpackb(data.encode(self.encoding), use_list=True, encoding=self.encoding)
