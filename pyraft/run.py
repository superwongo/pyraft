#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: run
@time: 2023/2/28
"""

import asyncio
from typing import Optional, Union, List, Tuple, Iterator

from pyraft.log import logger
from pyraft.server import Server
from pyraft.state import Leader, Follower, Candidate


def parser_server_str(
        servers: Union[str, Iterator[str], Iterator[Tuple[str, int]]]
) -> List[Tuple[str, int]]:
    server_list = []
    if isinstance(servers, str):
        for server in servers.split(','):
            host, port = server.rsplit(':', 1)
            server_list.append((host, int(port)))
    elif isinstance(servers, (list, tuple)):
        for server in servers:
            if isinstance(server, str):
                host, port = server.rsplit(':', 1)
                server_list.append((host, int(port)))
            elif isinstance(server, (list, tuple)):
                host, port = server
                server_list.append((host, int(port)))
    return server_list


def leader_listener(role: Leader):
    logger.info(f'当前服务切换为leader，ID为{role.id}')


def follower_listener(role: Follower):
    logger.info(f'当前服务切换为follower，ID为{role.id}')


def candidate_listener(role: Candidate):
    logger.info(f'当前服务切换为candidate，ID为{role.id}')


async def start(
        servers: Union[str, Iterator[str], Iterator[Tuple[str, int]]],
        current_server_index: int,
        loop: Optional[asyncio.AbstractEventLoop] = None
):
    loop = loop or asyncio.get_event_loop()
    servers = parser_server_str(servers)
    if current_server_index >= len(servers):
        raise IndexError(f'设置的当前服务器索引参数current_server_index[{current_server_index}]越界')
    current_host, current_port = servers.pop(current_server_index)
    current_server = Server(current_host, current_port, loop=loop)
    for host, port in servers:
        current_server.update_cluster(host, port)
    current_server.add_leader_listener(leader_listener)
    current_server.add_follower_listener(follower_listener)
    current_server.add_candidate_listener(candidate_listener)
    await current_server.start()


def stop():
    for server in Server.servers:
        server.stop()
