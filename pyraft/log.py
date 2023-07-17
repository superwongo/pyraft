#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: log
@time: 2023/2/24
"""

import logging

__all__ = ('logger',)


logger = logging.getLogger('raft')
# 日志级别：DEBUG|INFO|WARNING|ERROR
logger.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter('%(asctime)s | %(levelname)-8s | %(module)s:%(funcName)s:%(lineno)d - %(message)s')
)
logger.addHandler(handler)
