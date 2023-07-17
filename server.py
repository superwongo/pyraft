#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: wang_chao03
@project: pyraft
@file: server
@time: 2023/2/28
"""

import asyncio

from pyraft.run import start, stop
from pyraft.state import State
from pyraft.log import logger


LOCAL_IP = '127.0.0.1'


async def main(index):
    asyncio.ensure_future(start(
        servers=f'{LOCAL_IP}:8091,{LOCAL_IP}:8092,{LOCAL_IP}:8093', current_server_index=index or 0,
        loop=loop
    ))
    while True:
        server_id = State.get_server_id(LOCAL_IP, 8091 if index == 0 else 8092 if index == 1 else 8093)

        await State.wait_until_leader(server_id)

        await asyncio.sleep(5)

        if State.get_leader() == server_id:
            from datetime import datetime
            t = datetime.now().timestamp()
            logger.debug(f'准备设置参数[test]，值为{t}')
            await State.set_value('test', t)


if __name__ == '__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('-i', '--index', help="Current server's index", type=int, default=0)
    args = parser.parse_args()
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(main(args.index))
    except KeyboardInterrupt:
        stop()
